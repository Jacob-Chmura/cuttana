use std::collections::HashMap;
use std::hash::Hash;

/// Tracks vertex-based partion assignment output from a graph partioner
pub(crate) struct PartitionState<T> {
    pub assignments: HashMap<T, u8>,
    pub partition_sizes: Vec<u32>,
    pub max_partition_size: u32,
    pub num_partitions: u8,
    pub vertex_count: u64,
    pub edge_count: u64,
    pub cut_count: u64,
}

impl<T> PartitionState<T> {
    pub fn new(num_partitions: u8, max_partition_size: u32) -> Self {
        Self {
            assignments: HashMap::new(),
            partition_sizes: vec![0; num_partitions.into()],
            max_partition_size,
            num_partitions,
            vertex_count: 0,
            edge_count: 0,
            cut_count: 0,
        }
    }

    pub fn assign(&mut self, v: T, partition: u8)
    where
        T: Eq + Hash,
    {
        self.assignments.insert(v, partition);
        self.partition_sizes[partition as usize] += 1;
    }

    pub fn get_partition_of(&self, v: &T) -> Option<u8>
    where
        T: Eq + Hash,
    {
        self.assignments.get(v).copied()
    }

    pub fn smallest_partition(&self) -> u8 {
        self.partition_sizes
            .iter()
            .enumerate()
            .min_by_key(|&(_, sz)| sz)
            .unwrap()
            .0 as u8
    }

    pub fn has_room_in_partition(&self, partition: u8) -> bool {
        partition < self.num_partitions
            && self.partition_sizes[partition as usize] < self.max_partition_size
    }
}
