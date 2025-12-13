use crate::state::CuttanaState;
use std::collections::HashMap;

/// Final output of the partitioning algorithm.
pub struct PartitionResult<T> {
    pub assignments: HashMap<T, u8>,
    pub partition_sizes: Vec<u32>,
    pub vertex_count: u64,
    pub edge_count: u64,
    pub edge_cut_ratio: f64,
    pub communication_volume: f64,
}

impl<T> PartitionResult<T> {
    pub(crate) fn from_state(state: CuttanaState<T>) -> Self {
        Self {
            assignments: state.global.assignments,
            partition_sizes: state.global.partition_sizes,
            vertex_count: state.global.metrics.vertex_count,
            edge_count: state.global.metrics.edge_count,
            edge_cut_ratio: state.global.metrics.edge_cut_ratio(),
            communication_volume: state
                .global
                .metrics
                .communication_volume(state.global.num_partitions as u64),
        }
    }
}
