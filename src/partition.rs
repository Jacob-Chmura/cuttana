use crate::buffer::BufferManager;
use crate::stream::VertexStream;
use std::collections::HashMap;
use std::hash::Hash;

/// Tracks vertex-based partion assignment output from a graph partioner
struct PartitionState<T> {
    assignments: HashMap<T, usize>,
    partition_sizes: Vec<usize>,
}

impl<T> PartitionState<T> {
    pub fn new(num_partitions: usize) -> Self {
        Self {
            assignments: HashMap::new(),
            partition_sizes: vec![0; num_partitions],
        }
    }

    pub fn assign(&mut self, v: T, partition: usize)
    where
        T: Eq + Hash,
    {
        self.assignments.insert(v, partition);
        self.partition_sizes[partition] += 1;
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
}

pub fn cuttana_partition<T>(
    mut stream: impl VertexStream<VertexID = T>,
    num_partitions: usize,
    max_partition_size: usize,
    max_buffer_size: usize,
    degree_max: usize,
) -> HashMap<T, usize>
where
    T: Eq + Hash + Clone + Default,
{
    let mut buffer = BufferManager::<T>::new(max_buffer_size);
    let mut state = PartitionState::<T>::new(num_partitions);

    while let Some((v, nbrs)) = stream.next_vertex() {
        if nbrs.len() >= degree_max {
            partition_vertex(v, &nbrs, &mut state, &mut buffer, max_partition_size);
        } else {
            buffer.insert(v, &nbrs);
        }

        if buffer.is_at_capacity()
            && let Some((v, nbrs)) = buffer.evict()
        {
            partition_vertex(v, &nbrs, &mut state, &mut buffer, max_partition_size);
        }
    }

    while let Some((v, nbrs)) = buffer.evict() {
        partition_vertex(v, &nbrs, &mut state, &mut buffer, max_partition_size);
    }

    state.assignments
}

fn compute_balance_score<T>(state: &mut PartitionState<T>, partition: usize) -> f64 {
    // TODO: Make this generic
    const NUM_PARTITIONS: usize = 16;
    const GAMMA: f64 = 1.5;
    const VERTEX_COUNT: u32 = 100;
    const EDGE_COUNT: u32 = 100;

    let alpha: f64 = (NUM_PARTITIONS as f64).powf(GAMMA - 1f64) * (EDGE_COUNT as f64)
        / (VERTEX_COUNT as f64).powf(GAMMA);
    alpha * GAMMA * (state.partition_sizes[partition] as f64).powf(GAMMA - 1f64)
}

fn partition_vertex<T>(
    v: T,
    nbrs: &Vec<T>,
    state: &mut PartitionState<T>,
    buffer: &mut BufferManager<T>,
    max_partition_size: usize,
) where
    T: Eq + Hash + Clone + Default,
{
    // find best partition among the K partitions
    let (mut best_score, mut best_partition, mut tie_count) = (f64::NEG_INFINITY, 0, 0);

    let mut update_best_partition_candidate = |partition: usize, score: f64| {
        if score > best_score {
            (best_score, best_partition, tie_count) = (score, partition, 1);
        } else if score == best_score {
            tie_count += 1;
            // TODO: uniform integer sample [1, tie_count] inclusive with rng
            best_partition = partition;
        }
    };

    let mut nbrs_per_partition = vec![0; state.partition_sizes.len()];
    for nbr in nbrs {
        if let Some(&partition) = state.assignments.get(nbr)
            && state.partition_sizes[partition] < max_partition_size
        {
            nbrs_per_partition[partition] += 1;
            let score =
                nbrs_per_partition[partition] as f64 - compute_balance_score(state, partition);
            update_best_partition_candidate(partition, score);
        }
    }

    let partition = state.smallest_partition();
    update_best_partition_candidate(partition, -compute_balance_score(state, partition));

    state.assign(v, best_partition);

    for nbr in nbrs {
        buffer.update_score(nbr.clone());
    }
}
