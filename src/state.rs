use crate::config::CuttanaConfig;
use std::collections::HashMap;
use std::hash::Hash;

/// Partition quality metrics collected during partitioning
#[derive(Default, Debug, Clone)]
pub(crate) struct PartitionMetrics {
    pub vertex_count: u64,
    pub edge_count: u64,
    pub cut_count: u64,
}

impl PartitionMetrics {
    pub fn edge_cut_ratio(&self) -> f64 {
        if self.edge_count == 0 {
            return 0.0;
        }
        self.cut_count as f64 / self.edge_count as f64
    }

    pub fn communication_volume(&self, num_partitions: u64) -> f64 {
        if num_partitions == 0 || self.vertex_count == 0 {
            return 0.0;
        }
        self.cut_count as f64 / (num_partitions * self.vertex_count) as f64
    }
}

/// Tracks vertex-based partition assignment output from a graph partioner
pub(crate) struct PartitionCore<T, P> {
    pub assignments: HashMap<T, P>, // vertex -> partition id
    pub partition_sizes: Vec<u32>,
    pub num_partitions: P,
    pub balance_slack: f64,
    pub metrics: PartitionMetrics,
}

impl<T, P> PartitionCore<T, P>
where
    T: Eq + Hash,
    P: Copy + Into<usize> + TryFrom<usize>,
{
    pub fn new(num_partitions: P, balance_slack: f64) -> Self {
        Self {
            assignments: HashMap::new(),
            partition_sizes: vec![0; num_partitions.into()],
            num_partitions,
            balance_slack,
            metrics: PartitionMetrics::default(),
        }
    }

    pub fn assign_partition(&mut self, v: T, p: P) {
        let idx = p.into();
        self.assignments.insert(v, p);
        self.partition_sizes[idx] += 1;
    }

    pub fn partition_of(&self, v: &T) -> Option<P> {
        self.assignments.get(v).copied()
    }

    pub fn has_room_in_partition(&self, p: P) -> bool {
        let threshold = self.metrics.vertex_count as f64 / self.num_partitions.into() as f64;
        (self.partition_sizes[p.into()] as f64) < (1.0 + self.balance_slack) * threshold
    }

    pub fn has_room(&self) -> bool {
        for i in 0..self.partition_sizes.len() {
            if let Ok(p) = P::try_from(i)
                && self.has_room_in_partition(p)
            {
                return true;
            }
        }
        false
    }

    pub fn smallest_partition(&self) -> P {
        let idx = self
            .partition_sizes
            .iter()
            .enumerate()
            .min_by_key(|(_, sz)| *sz)
            .unwrap()
            .0;
        P::try_from(idx).ok().expect("Partition index overflow")
    }
}

pub(crate) struct SubPartition {
    pub parent: u8,
    pub size: u32,
    pub edges: HashMap<u16, u64>,
}

impl SubPartition {
    pub fn new(parent: u8, size: u32) -> Self {
        Self {
            parent,
            size,
            edges: HashMap::new(),
        }
    }

    pub fn add_edge(&mut self, other: u16) {
        *self.edges.entry(other).or_insert(0) += 1;
    }
}

/// Cuttana Partioning State
pub(crate) struct CuttanaState<T> {
    pub global: PartitionCore<T, u8>,
    pub global_to_sub: HashMap<u8, PartitionCore<T, u16>>,
    pub sub_partitions: Vec<SubPartition>,
    pub sub_in_partition: Vec<u16>,
    pub sub_edge_cut_by_partition: Vec<Vec<u64>>,
    pub _sub_move_score: Vec<Vec<i32>>, // TODO: Create segment trees
}

#[derive(Debug, Clone, Copy)]
enum UpdateType {
    Add,
    Remove,
    Update,
}

impl<T> CuttanaState<T>
where
    T: Eq + Hash,
{
    pub fn new(num_partitions: u8, config: &CuttanaConfig) -> Self {
        // Adding extra slack for `Phase 1` of the algorithm, letting `Phase 2` use the
        // user-specified slack parameter. E.g. see:
        // https://github.com/cuttana/cuttana-partitioner/blob/ed0c18251273a41792c1fc3e909d4ced44beaa27/partitioners/ogpart_single_thread.cpp#L167
        let balance_slack = (config.balance_slack * 2.0).min(config.balance_slack + 0.5);
        let global = PartitionCore::new(num_partitions, balance_slack);

        // Pre-create sub-partitions for each global partition
        let mut global_to_sub = HashMap::new();
        for g in 0..num_partitions {
            global_to_sub.insert(
                g,
                PartitionCore::new(config.num_sub_partitions, balance_slack),
            );
        }

        let total_sub_partitions = config.num_sub_partitions as u64 * num_partitions as u64;
        let sub_partitions = (0..total_sub_partitions)
            .map(|id| {
                let parent = (id / config.num_sub_partitions as u64) as u8;
                SubPartition::new(parent, config.num_sub_partitions.into())
            })
            .collect();

        Self {
            global,
            global_to_sub,
            sub_partitions,
            sub_in_partition: vec![config.num_sub_partitions; num_partitions as usize],
            sub_edge_cut_by_partition: vec![
                vec![0; num_partitions.into()];
                total_sub_partitions as usize
            ],
            _sub_move_score: vec![vec![0; num_partitions.into()]; num_partitions.into()],
        }
    }

    pub fn sub_partition(&mut self, global_partition: u8) -> &mut PartitionCore<T, u16> {
        self.global_to_sub
            .get_mut(&global_partition)
            .expect("Global partition does not exist")
    }

    pub fn update_metrics(&mut self, _v: &T, nbrs: &[T]) {
        self.global.metrics.vertex_count += 1;
        self.global.metrics.edge_count += nbrs.len() as u64;

        let v_eff = (self.global.metrics.vertex_count as f64 / self.global.num_partitions as f64)
            .round() as u64;
        let e_eff = (self.global.metrics.edge_count as f64 / self.global.num_partitions as f64)
            .round() as u64;

        for p in 0..self.global.num_partitions {
            self.sub_partition(p).metrics.vertex_count = v_eff;
            self.sub_partition(p).metrics.edge_count = e_eff;
        }
    }

    pub fn update_sub_edge_cut_by_partition(&mut self) {
        for (sub_idx, sub) in self.sub_partitions.iter().enumerate() {
            let row = &mut self.sub_edge_cut_by_partition[sub_idx];
            let mut total_cut = 0;

            // subtract edge weights for the partition of each adjacent sub
            for (&nbr, &weight) in &sub.edges {
                let nbr_parent = self.sub_partitions[nbr as usize].parent as usize;
                total_cut += weight;
                row[nbr_parent] = row[nbr_parent].saturating_sub(weight);
            }

            // add total edge cut to all partitions
            for val in row.iter_mut() {
                *val += total_cut;
            }
        }
    }

    pub fn get_sub_partition_graph_edge_weight(&self, src: u16, dst: u16) -> Option<u64> {
        self.sub_partitions[src as usize].edges.get(&dst).copied()
    }

    pub fn move_sub_partition(&mut self, sub: u16, from: u8, to: u8) {
        self.update_move_score_all_partitions(sub, UpdateType::Remove);

        let (sub_idx, from_idx, to_idx) = (sub as usize, from as usize, to as usize);

        // Update sub_edge_cut_by_partition using sub.edges
        for (adj_sub, edge_weight) in &self.sub_partitions[sub_idx].edges {
            self.sub_edge_cut_by_partition[*adj_sub as usize][to_idx] += edge_weight;
            self.sub_edge_cut_by_partition[*adj_sub as usize][from_idx] -= edge_weight;
        }

        // Update partition sizes
        let sub_size = self.sub_partitions[sub_idx].size;
        self.global.partition_sizes[from_idx] -= sub_size;
        self.global.partition_sizes[to_idx] += sub_size;

        // Update assignment and counts
        self.sub_partitions[sub_idx].parent = to;
        self.sub_in_partition[from_idx] -= 1;
        self.sub_in_partition[to_idx] += 1;

        // Build buckets of neighbors grouped by parent
        let mut buckets = vec![Vec::<u16>::new(); self.global.num_partitions as usize];
        for &adj_sub in self.sub_partitions[sub_idx].edges.keys() {
            let parent = self.sub_partitions[adj_sub as usize].parent as usize;
            buckets[parent].push(adj_sub);
        }

        // Update move scores
        for bucket in &buckets {
            for &adj_sub in bucket {
                let adj_parent = self.sub_partitions[adj_sub as usize].parent;
                if adj_parent == from || adj_parent == to {
                    self.update_move_score_all_partitions(adj_sub, UpdateType::Update);
                } else {
                    self.update_move_score(adj_sub, from, UpdateType::Update);
                    self.update_move_score(adj_sub, to, UpdateType::Update);
                }
            }
        }

        self.update_move_score_all_partitions(sub, UpdateType::Add);
    }

    fn update_move_score_all_partitions(&mut self, sub: u16, update: UpdateType) {
        for partition in 0..self.global.num_partitions {
            self.update_move_score(sub, partition, update);
        }
    }

    fn update_move_score(&mut self, sub: u16, adj_partition: u8, update: UpdateType) {
        let assigned_partition = self.sub_partitions[sub as usize].parent;
        let edge_cut = &self.sub_edge_cut_by_partition[sub as usize];
        let _delta = edge_cut[adj_partition as usize] - edge_cut[assigned_partition as usize];
        //let st_pos = &mut sub_to_segtree_ind[sub][adj_partition];
        match update {
            UpdateType::Add => {
                //*st_pos = move_score[assigned_partition][adj_partition.into()].add(sub, delta);
            }
            UpdateType::Remove => {
                //move_score[assigned_partition][adj_partition.into()].remove(sub, st_pos);
                //st_pos = -1;
            }
            UpdateType::Update => {
                //move_score[assigned_partition][adj_partition.into()].update(sub, delta, st_pos);
            }
        }
    }
}
