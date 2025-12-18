use crate::buffer::{BufferManager, CuttanaBufferScorer};
use crate::config::CuttanaConfig;
use crate::partition::{CuttanaPartitionScorer, Partitioner};
use crate::refine::Refiner;
use crate::result::PartitionResult;
use crate::state::CuttanaState;
use crate::stream::VertexStream;
use std::hash::Hash;

pub fn cuttana_partition<T>(
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
    let mut partitioner = Partitioner::<CuttanaPartitionScorer>::new(
        CuttanaPartitionScorer::new(config.gamma),
        CuttanaPartitionScorer::new(config.sub_gamma),
    );
    let mut state = CuttanaState::<T>::new(num_partitions, &config);

    for (v, nbrs) in stream {
        state.update_metrics(&v, &nbrs);

        if nbrs.len() as u32 >= config.buffer_degree_threshold {
            partitioner.partition(&v, &nbrs, &mut state);
            buffer.update_scores(&nbrs, &state);
        } else {
            buffer.insert(&v, &nbrs, &state);
        }

        if buffer.is_at_capacity()
            && let Some((v, nbrs)) = buffer.evict()
        {
            partitioner.partition(&v, &nbrs, &mut state);
            buffer.update_scores(&nbrs, &state);
        }
    }

    while let Some((v, nbrs)) = buffer.evict() {
        partitioner.partition(&v, &nbrs, &mut state);
        buffer.update_scores(&nbrs, &state);
    }

    let refiner = Refiner::new(&mut state, config.balance_slack, config.info_gain_threshold);
    refiner.fix_balance(&mut state);
    refiner.refine(&mut state);
    refiner.fix_balance(&mut state);

    PartitionResult::from_state(state)
}
