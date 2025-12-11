use crate::buffer::BufferManager;
use crate::scorer::{BalanceScorer, CuttanaBalanceScorer};
use crate::stream::VertexStream;
use std::collections::HashMap;
use std::hash::Hash;

/// Tracks vertex-based partion assignment output from a graph partioner
pub struct PartitionState<T> {
    assignments: HashMap<T, usize>,
    partition_sizes: Vec<usize>,
    max_partition_size: usize,
    pub num_partitions: usize,
}

impl<T> PartitionState<T> {
    pub fn new(num_partitions: usize, max_partition_size: usize) -> Self {
        Self {
            assignments: HashMap::new(),
            partition_sizes: vec![0; num_partitions],
            max_partition_size,
            num_partitions,
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
        // TODO: Switch from usize to std::Nonzero for unwrap
        self.partition_sizes
            .iter()
            .enumerate()
            .min_by_key(|&(_, sz)| sz)
            .unwrap()
            .0
    }

    pub fn has_room_in_partition(&self, partition: usize) -> bool {
        // TODO: Check out of bounds
        return self.partition_sizes[partition] < self.max_partition_size;
    }
}

pub fn cuttana_partition<T>(
    mut stream: impl VertexStream<VertexID = T>,
    num_partitions: usize,
    max_partition_size: usize,
    max_buffer_size: usize,
    degree_max: usize,
) -> HashMap<T, usize>
where
    T: Eq + Hash + Clone,
{
    let mut buffer = BufferManager::<T>::new(max_buffer_size);
    let mut state = PartitionState::<T>::new(num_partitions, max_partition_size);
    let balance_scorer = CuttanaBalanceScorer::new(num_partitions, 1.5f64, 100, 100);

    while let Some((v, nbrs)) = stream.next_entry() {
        if nbrs.len() >= degree_max {
            partition_vertex(&v, &nbrs, &mut state, &mut buffer, &balance_scorer);
        } else {
            buffer.insert(&v, &nbrs);
        }

        if buffer.is_at_capacity()
            && let Some((v, nbrs)) = buffer.evict()
        {
            partition_vertex(&v, &nbrs, &mut state, &mut buffer, &balance_scorer);
        }
    }

    while let Some((v, nbrs)) = buffer.evict() {
        partition_vertex(&v, &nbrs, &mut state, &mut buffer, &balance_scorer);
    }

    state.assignments
}

fn partition_vertex<T, B: BalanceScorer>(
    v: &T,
    nbrs: &Vec<T>,
    state: &mut PartitionState<T>,
    buffer: &mut BufferManager<T>,
    balance_scorer: &B,
) where
    T: Eq + Hash + Clone,
{
    let best_partition = balance_scorer.find_best_partition(v, nbrs, state);
    state.assign(v.clone(), best_partition);
    for nbr in nbrs {
        buffer.update_score(nbr);
    }
}
