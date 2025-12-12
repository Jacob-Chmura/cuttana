use crate::state::PartitionState;
use std::collections::HashMap;

/// Final output of the partitioning algorithm.
pub struct PartitionResult<T> {
    pub assignments: HashMap<T, u8>,
    pub partition_sizes: Vec<u32>,
    pub vertex_count: u64,
    pub edge_count: u64,
    pub edge_cut_cost: f64,
    pub communication_volume_cost: f64,
}

impl<T> PartitionResult<T> {
    pub(crate) fn from_state(state: PartitionState<T>) -> Self {
        Self {
            assignments: state.assignments,
            partition_sizes: state.partition_sizes,
            vertex_count: state.vertex_count,
            edge_count: state.edge_count,
            edge_cut_cost: state.cut_count as f64 / state.edge_count as f64,
            communication_volume_cost: state.cut_count as f64
                / (state.num_partitions as u64 * state.vertex_count) as f64,
        }
    }
}
