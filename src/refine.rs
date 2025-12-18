use crate::state::CuttanaState;
use core::f64;
use std::hash::Hash;

#[derive(Debug)]
struct SubPartitionMove {
    score: u64,
    sub: u16,
    from: u8,
    to: u8,
}

impl SubPartitionMove {
    #[inline]
    fn new(score: u64, sub: u16, from: u8, to: u8) -> Self {
        Self {
            score,
            sub,
            from,
            to,
        }
    }
}

pub(crate) struct Refiner {
    gain_threshold: u64,
    max_parent: u64,
    max_sub: u64,
    refine_capacity: u64,
}

impl Refiner {
    pub fn new<T: Hash + Ord>(
        state: &mut CuttanaState<T>,
        balance_slack: f64,
        gain_threshold: u64,
    ) -> Self {
        state.compute_sub_partition_edge_cuts();

        let num_vertices = state.global_assignments.metrics.vertex_count as f64;
        let max_parent =
            (num_vertices / state.num_partitions() as f64 * (1.0 + balance_slack)) as u64 + 1;
        let max_sub =
            (state.num_sub_partitions() as f64 / state.num_partitions() as f64 * 1.5) as u64 + 1;

        Self {
            gain_threshold,
            max_parent,
            max_sub,
            refine_capacity: (max_parent as f64 * 1.1) as u64,
        }
    }

    pub fn fix_balance<T: Hash + Ord>(&self, state: &mut CuttanaState<T>) {
        loop {
            let mut best_move: Option<SubPartitionMove> = None;

            for from_idx in 0..state.num_partitions() {
                if self.partition_within_vertex_capacity(state, from_idx) {
                    continue;
                }
                for to_idx in 0..state.num_partitions() {
                    if self.partition_at_vertex_capacity(state, to_idx)
                        || self.partition_at_sub_capacity(state, to_idx)
                    {
                        continue;
                    }

                    let (from, to) = (from_idx as u8, to_idx as u8);
                    let (score, sub) = (u64::MAX, 0u16); // move_score[p_u][p_v].get_min();
                    if self.sub_fits_in_partition(state, from, sub, to, self.max_parent)
                        && best_move.as_ref().is_none_or(|b| score < b.score)
                    {
                        best_move = Some(SubPartitionMove::new(score, sub, from, to));
                    }
                }
            }

            let Some(best_move) = best_move else {
                break;
            };

            state.move_sub_partition(best_move.sub, best_move.from, best_move.to);
        }
    }

    pub fn refine<T: Hash + Ord>(&self, state: &mut CuttanaState<T>) {
        loop {
            let mut moves: Option<(u64, Vec<SubPartitionMove>)> = None;

            for from_idx in 0..state.num_partitions() {
                if self.partition_is_empty(state, from_idx) {
                    continue;
                }

                for to_idx in 0..state.num_partitions() {
                    if from_idx == to_idx || self.partition_at_sub_capacity(state, to_idx) {
                        continue;
                    }

                    let (from, to) = (from_idx as u8, to_idx as u8);
                    let (score, sub) = (u64::MAX, 0u16); // move_score[p_u][p_v].get_min()

                    // Case 1: Direct move fits
                    if self.sub_fits_in_partition(state, from, sub, to, self.refine_capacity) {
                        if moves.as_ref().is_none_or(|m| score < m.0) {
                            moves = Some((score, vec![SubPartitionMove::new(score, sub, from, to)]))
                        }
                        continue;
                    }
                    // Case 2: Try secondary move (evict a sub partition)
                    else {
                        for evict_idx in 0..state.num_partitions() {
                            if to_idx == evict_idx
                                || self.partition_at_sub_capacity(state, evict_idx)
                            {
                                continue;
                            }

                            let (score_2, sub_2) = (0u64, 0u16); // move_score[p_v][p_w].get_min();
                            let effective_score = score
                                + score_2
                                + state.sub_partitions[sub as usize].get_edge(sub_2)
                                + if from_idx == evict_idx {
                                    state.sub_partitions[sub_2 as usize].get_edge(sub)
                                } else {
                                    0
                                };

                            if moves.as_ref().is_none_or(|m| effective_score < m.0) {
                                moves = Some((
                                    effective_score,
                                    vec![
                                        SubPartitionMove::new(score, sub, from, to),
                                        SubPartitionMove::new(score_2, sub_2, to, evict_idx as u8),
                                    ],
                                ))
                            }
                        }
                    }
                }
            }

            let (_, moves) = match moves {
                Some((score, moves)) if score <= self.gain_threshold => (score, moves),
                _ => break,
            };

            for m in moves {
                state.move_sub_partition(m.sub, m.from, m.to);
            }
        }
    }

    fn partition_is_empty<T: Hash + Ord>(&self, state: &CuttanaState<T>, p_idx: usize) -> bool {
        state.global_assignments.partition_sizes[p_idx] as u64 == 0
    }

    fn partition_at_vertex_capacity<T: Hash + Ord>(
        &self,
        state: &CuttanaState<T>,
        p_idx: usize,
    ) -> bool {
        state.global_assignments.partition_sizes[p_idx] as u64 >= self.max_parent
    }

    fn partition_within_vertex_capacity<T: Hash + Ord>(
        &self,
        state: &CuttanaState<T>,
        p_idx: usize,
    ) -> bool {
        state.global_assignments.partition_sizes[p_idx] as u64 <= self.max_parent
    }

    fn partition_at_sub_capacity<T: Hash + Ord>(
        &self,
        state: &CuttanaState<T>,
        p_idx: usize,
    ) -> bool {
        state.partitions[p_idx].num_sub as u64 >= self.max_sub
    }

    fn sub_fits_in_partition<T: Hash + Ord>(
        &self,
        state: &CuttanaState<T>,
        parent: u8,
        sub: u16,
        target: u8,
        limit: u64,
    ) -> bool {
        let sub_size = state.local_assignments[&parent].partition_sizes[sub as usize];
        let to_size = state.global_assignments.partition_sizes[target as usize];
        (to_size + sub_size) as u64 <= limit
    }
}
