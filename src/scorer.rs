use crate::state::PartitionAssignment;
use rand::Rng;
use rand::rngs::ThreadRng;
use std::hash::Hash;

pub(crate) trait PartitionScorer {
    fn find_best_partition<T: Eq + Hash + Clone, P: Copy + Into<usize> + TryFrom<usize>>(
        &mut self,
        v: &T,
        nbrs: &[T],
        core: &PartitionAssignment<T, P>,
    ) -> P;
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

    fn compute_score<T, P: Copy + Into<usize>>(
        &self,
        core: &PartitionAssignment<T, P>,
        partition: P,
    ) -> f64 {
        let partition_size = core.partition_sizes[partition.into()] as f64;
        let num_partitions = core.num_partitions.into() as f64;
        let vertex_count = core.metrics.vertex_count as f64;
        let edge_count = core.metrics.edge_count as f64;

        let alpha =
            num_partitions.powf(self.gamma - 1.0) * vertex_count / edge_count.powf(self.gamma);
        alpha * self.gamma * partition_size.powf(self.gamma - 1.0)
    }
}

impl PartitionScorer for CuttanaPartitionScorer {
    fn find_best_partition<T: Eq + Hash + Clone, P: Copy + Into<usize> + TryFrom<usize>>(
        &mut self,
        _v: &T,
        nbrs: &[T],
        core: &PartitionAssignment<T, P>,
    ) -> P {
        // First candidate is just smallest partition
        let mut best_partition = core.smallest_partition();
        let mut best_score = -self.compute_score(core, best_partition);
        let mut tie_count = 1;

        let mut update = |partition: P, score: f64, rng: &mut ThreadRng| {
            if score > best_score {
                (best_score, best_partition, tie_count) = (score, partition, 1);
            } else if score == best_score {
                tie_count += 1;
                if rng.random_ratio(1, tie_count) {
                    best_partition = partition;
                }
            }
        };

        let mut nbrs_per_partition = vec![0; core.num_partitions.into()];
        for nbr in nbrs {
            if let Some(partition) = core.partition_of(nbr)
                && core.has_room_in_partition(partition)
            {
                nbrs_per_partition[partition.into()] += 1;
                let score = nbrs_per_partition[partition.into()] as f64
                    - self.compute_score(core, partition);
                update(partition, score, &mut self.rng);
            }
        }

        best_partition
    }
}
