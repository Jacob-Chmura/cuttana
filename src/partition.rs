use crate::buffer::{BufferManager, BufferScorer, CuttanaBufferScorer};
use crate::result::PartitionResult;
use crate::scorer::{CuttanaPartitionScorer, PartitionScorer};
use crate::state::PartitionState;
use crate::stream::VertexStream;
use std::hash::Hash;

pub fn partition<T>(
    stream: VertexStream<T>,
    num_partitions: u8,
    max_partition_size: u32,
    max_buffer_size: u64,
    buffer_degree_threshold: u32,
) -> PartitionResult<T>
where
    T: Eq + Hash + Clone + Ord,
{
    assert!(num_partitions > 0, "Number of partitions must be > 0");
    assert!(max_partition_size > 0, "Max partition size must be > 0");

    const THETA: f64 = 2.0;
    let buffer_scorer = CuttanaBufferScorer::new(THETA, buffer_degree_threshold as f64);

    const GAMMA: f64 = 1.5;
    let mut scorer = CuttanaPartitionScorer::new(GAMMA);

    let mut buffer = BufferManager::<T, CuttanaBufferScorer>::new(max_buffer_size, buffer_scorer);
    let mut state = PartitionState::<T>::new(num_partitions, max_partition_size);

    for (v, nbrs) in stream {
        state.vertex_count += 1;
        state.edge_count += nbrs.len() as u64;

        if nbrs.len() as u32 >= buffer_degree_threshold {
            partition_vertex(&v, &nbrs, &mut state, &mut buffer, &mut scorer);
        } else {
            buffer.insert(&v, &nbrs, &state);
        }

        if buffer.is_at_capacity()
            && let Some((v, nbrs)) = buffer.evict()
        {
            partition_vertex(&v, &nbrs, &mut state, &mut buffer, &mut scorer);
        }
    }

    while let Some((v, nbrs)) = buffer.evict() {
        partition_vertex(&v, &nbrs, &mut state, &mut buffer, &mut scorer);
    }

    PartitionResult::from_state(state)
}

fn partition_vertex<T, B: PartitionScorer, S: BufferScorer>(
    v: &T,
    nbrs: &Vec<T>,
    state: &mut PartitionState<T>,
    buffer: &mut BufferManager<T, S>,
    scorer: &mut B,
) where
    T: Eq + Hash + Clone + Ord,
{
    if !state.has_room_in_partition(state.smallest_partition()) {
        panic!("Partition capacity exceeded. Increase max_partition_size or num_partitions.");
    }

    let best_partition = scorer.find_best_partition(v, nbrs, state);
    state.assign(v.clone(), best_partition);

    for nbr in nbrs {
        buffer.update_score(nbr, state);
        if let Some(nbr_partition) = state.get_partition_of(nbr)
            && nbr_partition != best_partition
        {
            state.cut_count += 1;
        }
    }
}
