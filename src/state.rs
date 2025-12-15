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
    pub max_partition_size: u32,
    pub metrics: PartitionMetrics,
}

impl<T, P> PartitionCore<T, P>
where
    T: Eq + Hash,
    P: Copy + Into<usize> + TryFrom<usize>,
{
    pub fn new(num_partitions: P, max_partition_size: u32) -> Self {
        Self {
            assignments: HashMap::new(),
            partition_sizes: vec![0; num_partitions.into()],
            num_partitions,
            max_partition_size,
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
        self.partition_sizes[p.into()] < self.max_partition_size
    }

    pub fn has_room(&self) -> bool {
        self.partition_sizes
            .iter()
            .any(|&sz| sz < self.max_partition_size)
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
    pub sub_partition_graph: HashMap<(u16, u16), u64>,
}

impl<T> CuttanaState<T>
where
    T: Eq + Hash,
{
    pub fn new(num_partitions: u8, max_partition_size: u32, config: &CuttanaConfig) -> Self {
        let global = PartitionCore::new(num_partitions, max_partition_size);

        // Pre-create sub-partitions for each global partition
        let mut global_to_sub = HashMap::new();
        for g in 0..num_partitions {
            global_to_sub.insert(
                g,
                PartitionCore::new(config.num_sub_partitions, config.max_sub_partition_size),
            );
        }

        Self {
            global,
            global_to_sub,
            sub_partition_graph: HashMap::new(),
        }
    }

    pub fn sub_partition(&mut self, global_partition: u8) -> &mut PartitionCore<T, u16> {
        self.global_to_sub
            .get_mut(&global_partition)
            .expect("Global partition does not exist")
    }
}
