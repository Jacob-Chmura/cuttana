use crate::state::PartitionState;
use rand::Rng;
use rand::rngs::ThreadRng;
use std::hash::Hash;

pub(crate) trait PartitionScorer {
    fn find_best_partition<T: Eq + Hash + Clone>(
        &mut self,
        v: &T,
        nbrs: &[T],
        state: &PartitionState<T>,
    ) -> u8;
}

pub(crate) struct CuttanaPartitionScorer {
    gamma: f64,
    rng: ThreadRng,
}

impl CuttanaPartitionScorer {
    pub fn new(gamma: f64) -> Self {
        Self {
            gamma,
            rng: rand::rng(),
        }
    }

    fn compute_score<T>(&self, state: &PartitionState<T>, _partition: u8) -> f64 {
        let num_partitions = state.num_partitions as f64;
        let alpha = num_partitions.powf(self.gamma - 1.0) * (state.edge_count as f64)
            / (state.vertex_count as f64).powf(self.gamma);
        alpha * self.gamma * num_partitions.powf(self.gamma - 1.0)
    }
}

impl PartitionScorer for CuttanaPartitionScorer {
    fn find_best_partition<T: Eq + Hash + Clone>(
        &mut self,
        _v: &T,
        nbrs: &[T],
        state: &PartitionState<T>,
    ) -> u8 {
        // First candidate is just smallest partition
        let mut best_partition = state.smallest_partition();
        let mut best_score = -self.compute_score(state, best_partition);
        let mut tie_count = 1;

        let mut update = |partition: u8, score: f64, rng: &mut ThreadRng| {
            if score > best_score {
                (best_score, best_partition, tie_count) = (score, partition, 1);
            } else if score == best_score {
                tie_count += 1;
                if rng.random_ratio(1, tie_count) {
                    best_partition = partition;
                }
            }
        };

        let mut nbrs_per_partition = vec![0; state.num_partitions.into()];
        for nbr in nbrs {
            if let Some(partition) = state.get_partition_of(nbr)
                && state.has_room_in_partition(partition)
            {
                nbrs_per_partition[partition as usize] += 1;
                let score = nbrs_per_partition[partition as usize] as f64
                    - self.compute_score(state, partition);
                update(partition, score, &mut self.rng);
            }
        }

        best_partition
    }
}
