use crate::metrics::PartitionMetrics;
use std::collections::HashMap;
use std::hash::Hash;

/// Tracks vertex-based partition assignment output from a graph partioner
pub(crate) struct PartitionAssignment<T, P> {
    pub assignments: HashMap<T, P>, // vertex -> partition id
    pub partition_sizes: Vec<u32>,
    pub num_partitions: P,
    pub metrics: PartitionMetrics,
    balance_slack: f64,
}

impl<T, P> PartitionAssignment<T, P>
where
    T: Eq + Hash,
    P: Copy + Into<usize> + TryFrom<usize>,
{
    pub fn new(num_partitions: P, balance_slack: f64) -> Self {
        Self {
            assignments: HashMap::new(),
            partition_sizes: vec![0; num_partitions.into()],
            num_partitions,
            balance_slack,
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
        let threshold = self.metrics.vertex_count as f64 / self.num_partitions.into() as f64;
        (self.partition_sizes[p.into()] as f64) < (1.0 + self.balance_slack) * threshold
    }

    pub fn has_room(&self) -> bool {
        for i in 0..self.partition_sizes.len() {
            if let Ok(p) = P::try_from(i)
                && self.has_room_in_partition(p)
            {
                return true;
            }
        }
        false
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
