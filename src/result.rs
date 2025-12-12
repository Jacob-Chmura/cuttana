use crate::state::PartitionState;
use std::collections::HashMap;

/// Final output of the partitioning algorithm.
pub struct PartitionResult<T> {
    pub assignments: HashMap<T, usize>,
    pub num_partitions: usize,
    pub vertex_count: usize,
    pub edge_count: usize,

    cut_count: usize,
}

impl<T> PartitionResult<T> {
    pub fn edge_cut_cost(&self) -> f64 {
        self.cut_count as f64 / self.edge_count as f64
    }

    pub fn communication_volume_cost(&self) -> f64 {
        self.cut_count as f64 / (self.num_partitions * self.vertex_count) as f64
    }

    pub(crate) fn from_state(state: PartitionState<T>) -> Self {
        Self {
            assignments: state.assignments,
            num_partitions: state.num_partitions,
            vertex_count: state.vertex_count,
            edge_count: state.edge_count,
            cut_count: state.cut_count,
        }
    }
}
