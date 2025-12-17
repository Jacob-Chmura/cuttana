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

pub(crate) fn run_refinement<T>(
    state: &mut CuttanaState<T>,
    max_parent: u64,
    max_sub: u64,
    gain_threshold: u64,
) where
    T: Hash + Ord,
{
    let refine_capacity = (max_parent as f64 * 1.1) as u64;

    loop {
        let mut moves: Option<(u64, Vec<SubPartitionMove>)> = None;

        for (from_idx, &from_size) in state.global.partition_sizes.iter().enumerate() {
            for (to_idx, &to_size) in state.global.partition_sizes.iter().enumerate() {
                let sub_in_to_partition = state.sub_in_partition[to_idx] as u64;
                if from_size == 0 || from_idx == to_idx || sub_in_to_partition >= max_sub {
                    continue;
                }

                let (from, to) = (from_idx as u8, to_idx as u8);
                let (score, sub) = (u64::MAX, 0u16); // move_score[p_u][p_v].get_min()
                let sub_size = state.global_to_sub[&from].partition_sizes[sub as usize];

                // Case 1: Direct move fits
                if (to_size + sub_size) as u64 <= refine_capacity {
                    if moves.as_ref().is_none_or(|m| score < m.0) {
                        moves = Some((score, vec![SubPartitionMove::new(score, sub, from, to)]))
                    }
                    continue;
                }
                // Case 2: Try secondary move (evict a sub partition out of to)
                else {
                    for (evict_idx, _) in state.global.partition_sizes.iter().enumerate() {
                        let sub_in_evict_partition = state.sub_in_partition[evict_idx] as u64;
                        if to_idx == evict_idx || sub_in_evict_partition >= max_sub {
                            continue;
                        }

                        let evict = evict_idx as u8;
                        let (score_2, sub_2) = (u64::MAX, 0u16); // move_score[p_v][p_w].get_min();
                        let mut score_3 = score + score_2;
                        score_3 += state
                            .get_sub_partition_graph_edge_weight(sub, sub_2)
                            .unwrap_or(0);

                        if from_idx == evict_idx {
                            score_3 += state
                                .get_sub_partition_graph_edge_weight(sub_2, sub)
                                .unwrap_or(0)
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
            Some((score, moves)) if score <= gain_threshold => (score, moves),
            _ => break,
        };

        for m in moves {
            state.move_sub_partition(m.sub, m.from, m.to);
        }
    }
}

pub(crate) fn fix_balance<T>(state: &mut CuttanaState<T>, max_parent: u64, max_sub: u64)
where
    T: Hash + Ord,
{
    loop {
        let mut best_move: Option<SubPartitionMove> = None;

        for (from_idx, &from_size) in state.global.partition_sizes.iter().enumerate() {
            if from_size as u64 <= max_parent {
                continue;
            }
            let from = from_idx as u8;

            for (to_idx, &to_size) in state.global.partition_sizes.iter().enumerate() {
                if to_size as u64 >= max_parent || state.sub_in_partition[to_idx] as u64 >= max_sub
                {
                    continue;
                }

                let to = to_idx as u8;
                let (score, sub) = (u64::MAX, 0u16); // move_score[p_u][p_v].get_min();
                let sub_size = state.global_to_sub[&from].partition_sizes[sub as usize];
                if (to_size + sub_size) as u64 > max_parent {
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
