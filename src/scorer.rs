use crate::partition::PartitionState;
use std::hash::Hash;

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
        alpha * self.gamma * (state.num_partitions as f64).powf(self.gamma - 1.0)
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

        let mut nbrs_per_partition = vec![0; state.num_partitions];
        for nbr in nbrs {
            if let Some(partition) = state.get_partition_of(nbr)
                && state.has_room_in_partition(partition)
            {
                nbrs_per_partition[partition] += 1;
                let score =
                    nbrs_per_partition[partition] as f64 - self.compute_score(state, partition);
                update(partition, score);
            }
        }

        best_partition
    }
}
