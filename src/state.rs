use crate::config::CuttanaConfig;
use crate::metrics::PartitionMetrics;
use std::collections::HashMap;
use std::hash::Hash;

/// Tracks vertex-based partition assignment output from a graph partioner
pub(crate) struct PartitionAssignment<T, P> {
    pub assignments: HashMap<T, P>, // vertex -> partition id
    pub partition_sizes: Vec<u32>,
    pub num_partitions: P,
    pub metrics: PartitionMetrics,
    balance_slack: f64,
}

impl<T, P> PartitionAssignment<T, P>
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

pub(crate) struct PartitionInfo {
    pub num_sub: u16,
    pub _move_score: Vec<i32>, // TODO: Segment Trees
}

impl PartitionInfo {
    pub fn new(num_partitions: u8, num_sub: u16) -> Self {
        Self {
            num_sub,
            _move_score: vec![0; num_partitions as usize],
        }
    }
}

pub(crate) struct SubPartitionInfo {
    pub parent: u8,
    pub edges: HashMap<u16, u64>,
    pub edge_cuts: Vec<u64>,
}

impl SubPartitionInfo {
    pub fn new(parent: u8, num_partitions: u8) -> Self {
        Self {
            parent,
            edges: HashMap::new(),
            edge_cuts: vec![0; num_partitions as usize],
        }
    }

    pub fn add_edge(&mut self, other: u16) {
        *self.edges.entry(other).or_insert(0) += 1;
    }
}

/// Cuttana Partioning State
pub(crate) struct CuttanaState<T> {
    pub global_assignments: PartitionAssignment<T, u8>,
    pub local_assignments: HashMap<u8, PartitionAssignment<T, u16>>,
    pub partitions: Vec<PartitionInfo>,
    pub sub_partitions: Vec<SubPartitionInfo>,
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
        let global = PartitionAssignment::new(num_partitions, balance_slack);

        // Pre-create sub-partitions for each global partition
        let mut global_to_sub = HashMap::new();
        for g in 0..num_partitions {
            global_to_sub.insert(
                g,
                PartitionAssignment::new(config.num_sub_partitions, balance_slack),
            );
        }

        let total_sub_partitions = config.num_sub_partitions as u64 * num_partitions as u64;
        let sub_partitions = (0..total_sub_partitions)
            .map(|id| {
                let parent = (id / config.num_sub_partitions as u64) as u8;
                SubPartitionInfo::new(parent, num_partitions)
            })
            .collect();

        let partitions = (0..num_partitions)
            .map(|_| PartitionInfo::new(num_partitions, config.num_sub_partitions))
            .collect();

        Self {
            global_assignments: global,
            local_assignments: global_to_sub,
            partitions,
            sub_partitions,
        }
    }

    pub fn local_assignment_for(
        &mut self,
        global_partition: u8,
    ) -> &mut PartitionAssignment<T, u16> {
        self.local_assignments
            .get_mut(&global_partition)
            .expect("Global partition does not exist")
    }

    pub fn update_metrics(&mut self, _v: &T, nbrs: &[T]) {
        self.global_assignments.metrics.vertex_count += 1;
        self.global_assignments.metrics.edge_count += nbrs.len() as u64;

        let v_eff = (self.global_assignments.metrics.vertex_count as f64
            / self.global_assignments.num_partitions as f64)
            .round() as u64;
        let e_eff = (self.global_assignments.metrics.edge_count as f64
            / self.global_assignments.num_partitions as f64)
            .round() as u64;

        for p in 0..self.global_assignments.num_partitions {
            self.local_assignment_for(p).metrics.vertex_count = v_eff;
            self.local_assignment_for(p).metrics.edge_count = e_eff;
        }
    }

    pub fn update_sub_edge_cut_by_partition(&mut self) {
        let parents: Vec<usize> = self
            .sub_partitions
            .iter()
            .map(|s| s.parent as usize)
            .collect();

        for i in 0..self.sub_partitions.len() {
            let sub = &mut self.sub_partitions[i];
            let edge_cuts = &mut sub.edge_cuts;

            edge_cuts.fill(0);
            let mut total_cut: u64 = 0;

            // subtract edge weights for the partition of each adjacent sub-partition
            for (&nbr, &weight) in &sub.edges {
                total_cut += weight;
                edge_cuts[parents[nbr as usize]] -= weight;
            }

            // add total edge cut to all partitions
            edge_cuts.iter_mut().for_each(|x| *x += total_cut);
        }
    }

    pub fn get_sub_partition_graph_edge_weight(&self, src: u16, dst: u16) -> Option<u64> {
        self.sub_partitions[src as usize].edges.get(&dst).copied()
    }

    pub fn move_sub_partition(&mut self, sub: u16, from: u8, to: u8) {
        self.update_move_score_all_partitions(sub, UpdateType::Remove);

        let (sub_idx, from_idx, to_idx) = (sub as usize, from as usize, to as usize);

        let edges: Vec<(usize, u64)> = self.sub_partitions[sub_idx]
            .edges
            .iter()
            .map(|(s, w)| (*s as usize, *w))
            .collect();

        // Update sub_edge_cut_by_partition using sub.edges
        for (adj_sub, edge_weight) in edges {
            let edge_cuts = &mut self.sub_partitions[adj_sub].edge_cuts;
            edge_cuts[to_idx] += edge_weight;
            edge_cuts[from_idx] -= edge_weight;
        }

        // Update partition sizes
        let sub_size = self.local_assignment_for(from).partition_sizes[sub_idx];
        self.global_assignments.partition_sizes[from_idx] -= sub_size;
        self.global_assignments.partition_sizes[to_idx] += sub_size;

        // Update assignment and counts
        self.sub_partitions[sub_idx].parent = to;
        self.partitions[from_idx].num_sub -= 1;
        self.partitions[to_idx].num_sub += 1;

        // Build buckets of neighbors grouped by parent
        let mut buckets = vec![Vec::<u16>::new(); self.global_assignments.num_partitions as usize];
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
        for partition in 0..self.global_assignments.num_partitions {
            self.update_move_score(sub, partition, update);
        }
    }

    fn update_move_score(&mut self, sub: u16, adj_partition: u8, update: UpdateType) {
        let assigned_partition = self.sub_partitions[sub as usize].parent;
        let edge_cut = &self.sub_partitions[sub as usize].edge_cuts;
        let _delta = edge_cut[adj_partition as usize] - edge_cut[assigned_partition as usize];

        // TODO: Segment tree updates
        match update {
            UpdateType::Add => {}
            UpdateType::Remove => {}
            UpdateType::Update => {}
        }
    }
}
