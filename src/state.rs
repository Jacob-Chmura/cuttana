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

/// Cuttana Partioning State
pub(crate) struct CuttanaState<T> {
    pub global: PartitionCore<T, u8>,
    pub global_to_sub: HashMap<u8, PartitionCore<T, u16>>,
    pub sub_partition_graph: Vec<HashMap<u16, u64>>,
    pub _sub_move_score: Vec<Vec<i32>>, // TODO: Create segment trees
    pub sub_in_partition: Vec<u16>,
    pub sub_to_partition: Vec<u8>,
    pub sub_edge_cut_by_partition: Vec<Vec<u64>>,
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

        Self {
            global,
            global_to_sub,
            sub_partition_graph: {
                let total_sub_partitions = config.num_sub_partitions as u64 * num_partitions as u64;
                vec![HashMap::new(); total_sub_partitions as usize]
            },
            _sub_move_score: vec![vec![0; num_partitions.into()]; num_partitions.into()],
            sub_in_partition: vec![config.num_sub_partitions; num_partitions.into()],
            sub_to_partition: {
                let total_sub_partitions = config.num_sub_partitions as u64 * num_partitions as u64;
                (0..total_sub_partitions)
                    .map(|i| (i / config.num_sub_partitions as u64) as u8)
                    .collect()
            },
            sub_edge_cut_by_partition: {
                let total_sub_partitions = config.num_sub_partitions as u64 * num_partitions as u64;
                vec![vec![0; num_partitions.into()]; total_sub_partitions as usize]
            },
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
}
