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
}

impl Refiner {
    pub fn new<T: Hash + Ord>(
        state: &mut CuttanaState<T>,
        balance_slack: f64,
        gain_threshold: u64,
    ) -> Self {
        state.compute_sub_partition_edge_cuts();

        let num_partitions = state.global_assignments.num_partitions as f64;
        let num_sub_partitions = state.local_assignment_for(0).num_partitions as f64;
        let num_vertices = state.global_assignments.metrics.vertex_count as f64;
        let max_parent = (num_vertices / num_partitions * (1.0 + balance_slack)) as u64 + 1;
        let max_sub = (num_sub_partitions / num_partitions * 1.5) as u64 + 1;

        Self {
            gain_threshold,
            max_parent,
            max_sub,
        }
    }

    pub fn fix_balance<T: Hash + Ord>(&self, state: &mut CuttanaState<T>) {
        loop {
            let mut best_move: Option<SubPartitionMove> = None;

            for (from_idx, &from_size) in
                state.global_assignments.partition_sizes.iter().enumerate()
            {
                if from_size as u64 <= self.max_parent {
                    continue;
                }
                let from = from_idx as u8;

                for (to_idx, &to_size) in
                    state.global_assignments.partition_sizes.iter().enumerate()
                {
                    if to_size as u64 >= self.max_parent
                        || state.partitions[to_idx].num_sub as u64 >= self.max_sub
                    {
                        continue;
                    }

                    let to = to_idx as u8;
                    let (score, sub) = (u64::MAX, 0u16); // move_score[p_u][p_v].get_min();
                    let sub_size = state.local_assignments[&from].partition_sizes[sub as usize];
                    if (to_size + sub_size) as u64 > self.max_parent {
                        continue;
                    }

                    if best_move.as_ref().is_none_or(|b| score < b.score) {
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
        let refine_capacity = (self.max_parent as f64 * 1.1) as u64;

        loop {
            let mut moves: Option<(u64, Vec<SubPartitionMove>)> = None;

            for (from_idx, &from_size) in
                state.global_assignments.partition_sizes.iter().enumerate()
            {
                for (to_idx, &to_size) in
                    state.global_assignments.partition_sizes.iter().enumerate()
                {
                    let sub_in_to_partition = state.partitions[to_idx].num_sub as u64;
                    if from_size == 0 || from_idx == to_idx || sub_in_to_partition >= self.max_sub {
                        continue;
                    }

                    let (from, to) = (from_idx as u8, to_idx as u8);
                    let (score, sub) = (u64::MAX, 0u16); // move_score[p_u][p_v].get_min()
                    let sub_size = state.local_assignments[&from].partition_sizes[sub as usize];

                    // Case 1: Direct move fits
                    if (to_size + sub_size) as u64 <= refine_capacity {
                        if moves.as_ref().is_none_or(|m| score < m.0) {
                            moves = Some((score, vec![SubPartitionMove::new(score, sub, from, to)]))
                        }
                        continue;
                    }
                    // Case 2: Try secondary move (evict a sub partition out of to)
                    else {
                        for (evict_idx, _) in
                            state.global_assignments.partition_sizes.iter().enumerate()
                        {
                            let sub_in_evict_partition = state.partitions[evict_idx].num_sub as u64;
                            if to_idx == evict_idx || sub_in_evict_partition >= self.max_sub {
                                continue;
                            }

                            let evict = evict_idx as u8;
                            let (score_2, sub_2) = (u64::MAX, 0u16); // move_score[p_v][p_w].get_min();
                            let mut score_3 = score + score_2;
                            score_3 += state.sub_partitions[sub as usize].get_edge(sub_2);

                            if from_idx == evict_idx {
                                score_3 += state.sub_partitions[sub_2 as usize].get_edge(sub);
                            }

                            if moves.as_ref().is_none_or(|m| score_3 < m.0) {
                                moves = Some((
                                    score_3,
                                    vec![
                                        SubPartitionMove::new(score, sub, from, to),
                                        SubPartitionMove::new(score_2, sub_2, to, evict),
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
}
