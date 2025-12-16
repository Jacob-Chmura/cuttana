use crate::buffer::{BufferManager, BufferScorer, CuttanaBufferScorer};
use crate::config::CuttanaConfig;
use crate::refine::{fix_balance, run_refinement};
use crate::result::PartitionResult;
use crate::scorer::{CuttanaPartitionScorer, PartitionScorer};
use crate::state::CuttanaState;
use crate::stream::VertexStream;
use std::hash::Hash;

pub fn partition<T>(
    stream: VertexStream<T>,
    num_partitions: u8,
    config: CuttanaConfig,
) -> PartitionResult<T>
where
    T: Eq + Hash + Clone + Ord,
{
    assert!(num_partitions > 0, "Number of partitions must be > 0");

    let mut buffer = BufferManager::new(
        config.max_buffer_size,
        CuttanaBufferScorer::new(config.theta, config.buffer_degree_threshold),
    );

    let mut scorer = CuttanaPartitionScorer::new(config.gamma);
    let mut sub_scorer = CuttanaPartitionScorer::new(config.sub_gamma);
    let mut state = CuttanaState::<T>::new(num_partitions, &config);

    for (v, nbrs) in stream {
        state.update_metrics(&v, &nbrs);

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

    let num_partitions = state.global.num_partitions as f64;
    let num_sub_partitions = state.sub_partition(0).num_partitions as f64;
    let num_vertices = state.global.metrics.vertex_count as f64;
    let max_parent = (num_vertices / num_partitions * (1.0 + config.balance_slack)) as u64 + 1;
    let max_sub = (num_sub_partitions / num_partitions * 1.5) as u64 + 1;

    state.update_sub_edge_cut_by_partition();
    fix_balance(&mut state, max_parent, max_sub);
    run_refinement(&mut state, max_parent, max_sub, config.info_gain_threshold);
    fix_balance(&mut state, max_parent, max_sub);
    PartitionResult::from_state(state)
}

fn partition_vertex<T, B: PartitionScorer, S: BufferScorer>(
    v: &T,
    nbrs: &[T],
    state: &mut CuttanaState<T>,
    buffer: &mut BufferManager<T, S>,
    scorer: &mut B,
    sub_scorer: &mut B,
) where
    T: Eq + Hash + Clone + Ord,
{
    if !state.global.has_room() {
        // TODO: Return result and graceful handle
        panic!("Partition capacity exceeded. Increase balance_slack or num_partitions.");
    }

    let best_partition = scorer.find_best_partition(v, nbrs, &state.global);
    state.global.assign_partition(v.clone(), best_partition);

    for nbr in nbrs {
        buffer.update_score(nbr, state);
        if let Some(nbr_partition) = state.global.partition_of(nbr)
            && nbr_partition != best_partition
        {
            state.global.metrics.cut_count += 1;
        }
    }

    let best_sub_partition: u16 =
        sub_scorer.find_best_partition(v, nbrs, state.sub_partition(best_partition));
    state
        .sub_partition(best_partition)
        .assign_partition(v.clone(), best_sub_partition);

    for nbr in nbrs {
        if let Some(nbr_sub_partition) = state.sub_partition(best_partition).partition_of(nbr)
            && nbr_sub_partition != best_sub_partition
        {
            let src = (best_partition as u64 * state.sub_partition(0).num_partitions as u64)
                + best_sub_partition as u64;
            let dst = (best_partition as u64 * state.sub_partition(0).num_partitions as u64)
                + nbr_sub_partition as u64;

            *state.sub_partition_graph[src as usize]
                .entry(dst as u16)
                .or_insert(0) += 1;
            *state.sub_partition_graph[dst as usize]
                .entry(src as u16)
                .or_insert(0) += 1;
        }
    }
}
