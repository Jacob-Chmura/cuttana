use crate::partition::PartitionState;
use rand::Rng;
use rand::rngs::ThreadRng;
use std::hash::Hash;

pub trait PartitionScorer {
    fn find_best_partition<T: Eq + Hash + Clone>(
        &mut self,
        v: &T,
        nbrs: &Vec<T>,
        state: &PartitionState<T>,
    ) -> usize;
}

pub struct CuttanaPartitionScorer {
    gamma: f64,
    vertex_count: usize,
    edge_count: usize,
    rng: ThreadRng,
}

impl CuttanaPartitionScorer {
    pub fn new(gamma: f64) -> Self {
        Self {
            gamma,
            vertex_count: 0,
            edge_count: 0,
            rng: rand::rng(),
        }
    }

    fn compute_score<T>(&self, state: &PartitionState<T>, _partition: usize) -> f64 {
        let num_partitions = state.num_partitions as f64;
        let alpha = num_partitions.powf(self.gamma - 1.0) * (self.edge_count as f64)
            / (self.vertex_count as f64).powf(self.gamma);
        alpha * self.gamma * num_partitions.powf(self.gamma - 1.0)
    }
}

impl PartitionScorer for CuttanaPartitionScorer {
    fn find_best_partition<T: Eq + Hash + Clone>(
        &mut self,
        _v: &T,
        nbrs: &Vec<T>,
        state: &PartitionState<T>,
    ) -> usize {
        // Update approximate vertex/edge counts
        self.vertex_count += 1;
        self.edge_count += nbrs.len();

        // First candidate is just smallest partition
        let mut best_partition = state.smallest_partition();
        let mut best_score = -self.compute_score(state, best_partition);
        let mut tie_count = 1;

        let mut update = |partition: usize, score: f64, rng: &mut ThreadRng| {
            if score > best_score {
                (best_score, best_partition, tie_count) = (score, partition, 1);
            } else if score == best_score {
                tie_count += 1;
                if rng.random_ratio(1, tie_count) {
                    best_partition = partition;
                }
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
                update(partition, score, &mut self.rng);
            }
        }

        best_partition
    }
}
