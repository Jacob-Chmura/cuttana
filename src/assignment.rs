use std::collections::HashMap;
use std::hash::Hash;

/// Tracks vertex-based partition assignment output from a graph partioner
pub(crate) struct PartitionAssignment<T, P> {
    pub assignments: HashMap<T, P>, // vertex -> partition id
    pub partition_sizes: Vec<usize>,
    pub num_partitions: usize,
    pub metrics: PartitionMetrics,
    balance_slack: f64,
}

impl<T, P> PartitionAssignment<T, P>
where
    T: Eq + Hash,
    P: Copy + Into<usize> + TryFrom<usize>,
{
    pub fn new(num_partitions: usize, balance_slack: f64) -> Self {
        Self {
            assignments: HashMap::new(),
            partition_sizes: vec![0; num_partitions],
            num_partitions,
            balance_slack,
            metrics: PartitionMetrics::new(num_partitions),
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
        let threshold = self.metrics.vertex_count as f64 / self.num_partitions as f64;
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

/// Partition quality metrics collected during partitioning
#[derive(Debug, Clone)]
pub(crate) struct PartitionMetrics {
    pub vertex_count: u64,
    pub edge_count: u64,
    pub cut_count: u64,
    num_partitions: usize,
}

impl PartitionMetrics {
    pub fn new(num_partitions: usize) -> Self {
        assert!(num_partitions > 0, "Number of partitions must be > 0");

        Self {
            vertex_count: 0,
            edge_count: 0,
            cut_count: 0,
            num_partitions,
        }
    }

    pub fn edge_cut_ratio(&self) -> f64 {
        if self.edge_count == 0 {
            return 0.0;
        }
        self.cut_count as f64 / self.edge_count as f64
    }

    pub fn communication_volume(&self) -> f64 {
        if self.vertex_count == 0 {
            return 0.0;
        }
        self.cut_count as f64 / (self.num_partitions as u64 * self.vertex_count) as f64
    }
}
