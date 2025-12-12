use std::collections::HashMap;
use std::hash::Hash;

/// Tracks vertex-based partion assignment output from a graph partioner
pub(crate) struct PartitionState<T> {
    pub assignments: HashMap<T, usize>,
    pub partition_sizes: Vec<usize>,
    pub max_partition_size: usize,
    pub num_partitions: usize,
    pub vertex_count: usize,
    pub edge_count: usize,
    pub cut_count: usize,
}

impl<T> PartitionState<T> {
    pub fn new(num_partitions: usize, max_partition_size: usize) -> Self {
        Self {
            assignments: HashMap::new(),
            partition_sizes: vec![0; num_partitions],
            max_partition_size,
            num_partitions,
            vertex_count: 0,
            edge_count: 0,
            cut_count: 0,
        }
    }

    pub fn assign(&mut self, v: T, partition: usize)
    where
        T: Eq + Hash,
    {
        self.assignments.insert(v, partition);
        self.partition_sizes[partition] += 1;
    }

    pub fn get_partition_of(&self, v: &T) -> Option<usize>
    where
        T: Eq + Hash,
    {
        self.assignments.get(v).copied()
    }

    pub fn smallest_partition(&self) -> usize {
        self.partition_sizes
            .iter()
            .enumerate()
            .min_by_key(|&(_, sz)| sz)
            .unwrap()
            .0
    }

    pub fn has_room_in_partition(&self, partition: usize) -> bool {
        partition < self.num_partitions && self.partition_sizes[partition] < self.max_partition_size
    }
}
