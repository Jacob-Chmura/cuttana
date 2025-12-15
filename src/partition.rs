use crate::buffer::{BufferManager, BufferScorer, CuttanaBufferScorer};
use crate::config::CuttanaConfig;
use crate::result::PartitionResult;
use crate::scorer::{CuttanaPartitionScorer, PartitionScorer};
use crate::state::CuttanaState;
use crate::stream::VertexStream;
use std::hash::Hash;

pub fn partition<T>(
    stream: VertexStream<T>,
    num_partitions: u8,
    max_partition_size: u32,
    config: CuttanaConfig,
) -> PartitionResult<T>
where
    T: Eq + Hash + Clone + Ord,
{
    assert!(num_partitions > 0, "Number of partitions must be > 0");
    assert!(max_partition_size > 0, "Max partition size must be > 0");

    let mut buffer = BufferManager::new(
        config.max_buffer_size,
        CuttanaBufferScorer::new(config.theta, config.buffer_degree_threshold),
    );

    let mut scorer = CuttanaPartitionScorer::new(config.gamma);
    let mut sub_scorer = CuttanaPartitionScorer::new(config.sub_gamma);
    let mut state = CuttanaState::<T>::new(num_partitions, max_partition_size, &config);

    for (v, nbrs) in stream {
        // TODO: Organize
        state.global.metrics.vertex_count += 1;
        state.global.metrics.edge_count += nbrs.len() as u64;

        if nbrs.len() as u32 >= config.buffer_degree_threshold {
            partition_vertex(
                &v,
                &nbrs,
                &mut state,
                &mut buffer,
                &mut scorer,
                &mut sub_scorer,
            );
        } else {
            buffer.insert(&v, &nbrs, &state);
        }

        if buffer.is_at_capacity()
            && let Some((v, nbrs)) = buffer.evict()
        {
            partition_vertex(
                &v,
                &nbrs,
                &mut state,
                &mut buffer,
                &mut scorer,
                &mut sub_scorer,
            );
        }
    }

    while let Some((v, nbrs)) = buffer.evict() {
        partition_vertex(
            &v,
            &nbrs,
            &mut state,
            &mut buffer,
            &mut scorer,
            &mut sub_scorer,
        );
    }

    PartitionResult::from_state(state)
}

fn partition_vertex<T, B: PartitionScorer, S: BufferScorer>(
    v: &T,
    nbrs: &Vec<T>,
    state: &mut CuttanaState<T>,
    buffer: &mut BufferManager<T, S>,
    scorer: &mut B,
    _sub_scorer: &mut B,
) where
    T: Eq + Hash + Clone + Ord,
{
    if !state.global.has_room() {
        // TODO: Return result and graceful handle
        panic!("Partition capacity exceeded. Increase max_partition_size or num_partitions.");
    }

    let best_partition = scorer.find_best_partition(v, nbrs, state);
    state.global.assign_partition(v.clone(), best_partition);

    for nbr in nbrs {
        buffer.update_score(nbr, state);
        if let Some(nbr_partition) = state.global.partition_of(nbr)
            && nbr_partition != best_partition
        {
            state.global.metrics.cut_count += 1;
        }
    }

    // TODO: Get sub scorer
    let best_sub_partition: u16 = 0;
    state
        .sub_partition(best_partition)
        .assign_partition(v.clone(), best_sub_partition);

    for nbr in nbrs {
        if let Some(nbr_sub_partition) = state.sub_partition(best_partition).partition_of(nbr)
            && nbr_sub_partition != best_sub_partition
        {
            *state
                .sub_partition_graph
                .entry((best_sub_partition, nbr_sub_partition))
                .or_insert(0) += 1;
            *state
                .sub_partition_graph
                .entry((nbr_sub_partition, best_sub_partition))
                .or_insert(0) += 1;
        }
    }
}
