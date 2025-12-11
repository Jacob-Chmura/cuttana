use crate::buffer::BufferManager;
use crate::stream::VertexStream;
use std::collections::HashMap;
use std::hash::Hash;

/// Tracks vertex-based partion assignment output from a graph partioner
pub struct PartitionState<T> {
    assignments: HashMap<T, usize>,
    partition_sizes: Vec<usize>,
    max_partition_size: usize,
}

impl<T> PartitionState<T> {
    pub fn new(num_partitions: usize, max_partition_size: usize) -> Self {
        Self {
            assignments: HashMap::new(),
            partition_sizes: vec![0; num_partitions],
            max_partition_size,
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

    pub fn has_room_in_partition_of(&self, v: &T) -> bool
    where
        T: Eq + Hash,
    {
        matches!(self.assignments.get(v), Some(&partition) if self.partition_sizes[partition] < self.max_partition_size)
    }
}

pub trait BalanceScorer {
    fn find_best_partition<T: Eq + Hash + Clone>(
        &self,
        v: &T,
        nbrs: &Vec<T>,
        state: &PartitionState<T>,
    ) -> usize;
}

pub struct CuttanaBalanceScorer {
    num_partitions: usize,
    gamma: f64,
    vertex_count: usize,
    edge_count: usize,
}

impl CuttanaBalanceScorer {
    pub fn new(num_partitions: usize, gamma: f64, vertex_count: usize, edge_count: usize) -> Self {
        Self {
            num_partitions,
            gamma,
            vertex_count,
            edge_count,
        }
    }

    fn compute_score<T>(&self, state: &PartitionState<T>, partition: usize) -> f64 {
        let alpha = (self.num_partitions as f64).powf(self.gamma - 1.0) * (self.edge_count as f64)
            / (self.vertex_count as f64).powf(self.gamma);
        alpha * self.gamma * (state.partition_sizes[partition] as f64).powf(self.gamma - 1.0)
    }
}

impl BalanceScorer for CuttanaBalanceScorer {
    fn find_best_partition<T: Eq + Hash + Clone>(
        &self,
        v: &T,
        nbrs: &Vec<T>,
        state: &PartitionState<T>,
    ) -> usize {
        // First candidate is just smallest partition
        let mut best_partition = state.smallest_partition();
        let mut best_score = -self.compute_score(state, best_partition);
        let mut tie_count = 1;

        let mut update = |partition: usize, score: f64| {
            if score > best_score {
                (best_score, best_partition, tie_count) = (score, partition, 1);
            } else if score == best_score {
                // TODO: uniform integer sample [1, tie_count] inclusive with rng
                tie_count += 1;
                best_partition = partition;
            }
        };

        let mut nbrs_per_partition = vec![0; state.partition_sizes.len()];
        for nbr in nbrs {
            if state.has_room_in_partition_of(nbr) {
                let partition = state.assignments[nbr];
                nbrs_per_partition[partition] += 1;
                let score =
                    nbrs_per_partition[partition] as f64 - self.compute_score(state, partition);
                update(partition, score);
            }
        }

        best_partition
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
    T: Eq + Hash + Clone + Default,
{
    let best_partition = balance_scorer.find_best_partition(v, nbrs, state);
    state.assign(v.clone(), best_partition);
    for nbr in nbrs {
        buffer.update_score(nbr);
    }
}
