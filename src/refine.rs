use crate::state::CuttanaState;
use core::f64;
use std::hash::Hash;

#[derive(Debug, Clone, Copy)]
enum UpdateType {
    ADD,
    REMOVE,
    UPDATE,
}

pub(crate) fn refine<T>(state: &mut CuttanaState<T>, info_gain_threshold: f64)
where
    T: Eq + Hash + Clone + Ord,
{
    let p2_balance_slack = 0.05;
    let refine_capacity: u64 = ((state.global.metrics.vertex_count as f64
        / state.global.num_partitions as f64)
        * (1.0 + p2_balance_slack)
        + 1.0) as u64;
    let max_sub_in_partition = (state.sub_partition(0).num_partitions as f64
        / state.global.num_partitions as f64
        * 1.5) as u64;
    let refine_capacity_ub = (refine_capacity as f64 * 1.1) as u64;

    let mut sub_in_partition =
        vec![state.sub_partition(0).num_partitions as u64; state.global.num_partitions.into()];

    let mut sub_edge_cut_by_partition = vec![
        vec![0; state.sub_partition(0).num_partitions.into()];
        state.global.num_partitions.into()
    ];
    //for (int sub_id = 0; sub_id < SUB_PARTITION_COUNT; sub_id++)
    //{
    //    int sub_total_edge_cut = 0;
    //    for (auto &[adj_sub_id, edge_weight] : sub_partition_graph[sub_id])
    //    {
    //        sub_total_edge_cut += edge_weight;
    //        sub_edge_cut_by_partition[sub_id][sub_to_partition[adj_sub_id]] -= edge_weight;
    //    }

    //    // set edge cut for each partition to be the total edge cut of sub partition
    //    for (int partition_id = 0; partition_id < PARTITION_COUNT; partition_id++)
    //    {
    //        sub_edge_cut_by_partition[sub_id][partition_id] += sub_total_edge_cut;
    //    }
    //}

    fix_balance(
        state,
        sub_in_partition,
        sub_edge_cut_by_partition,
        refine_capacity,
        max_sub_in_partition,
    );

    loop {
        let mut best_score = f64::INFINITY;
        let mut partitions_to_move: Vec<(u16, u8, u8)> = Vec::new(); // sub, from_partition,
        // to_partition

        for (p_u, &p_u_sz) in state.global.partition_sizes.iter().enumerate() {
            for (p_v, &p_v_sz) in state.global.partition_sizes.iter().enumerate() {
                if p_u_sz == 0 || p_u == p_v || sub_in_partition[p_v] >= max_sub_in_partition {
                    continue;
                }

                let (score, sub_u) = move_score[p_u][p_v].get_min();
                let sub_u_size = state.sub_partition(p_u as u8).partition_sizes[sub_u as usize];

                // Case 1: direct move fits
                if (p_v_sz + sub_u_size) as u64 <= refine_capacity_ub {
                    if (score as f64) < best_score {
                        best_score = score;
                        partitions_to_move = vec![(sub_u, p_u as u8, p_v as u8)];
                    }
                }
                // Case 2: overflow: try secondary move p_v -> p_w
                else {
                    for (p_w, p_w_sz) in state.global.partition_sizes.iter().enumerate() {
                        if p_w == p_v || sub_in_partition[p_w] >= max_sub_in_partition {
                            continue;
                        }

                        let (score_2, sub_v) = move_score[p_v][p_w].get_min();
                        let mut effective_score = score + score_2;

                        effective_score += state
                            .sub_partition_graph
                            .get(&(sub_u, sub_v))
                            .copied()
                            .unwrap_or(0) as f64;

                        if p_w == p_u {
                            effective_score += state
                                .sub_partition_graph
                                .get(&(sub_v, sub_u))
                                .copied()
                                .unwrap_or(0) as f64;
                        }

                        if effective_score < best_score {
                            best_score = score;
                            partitions_to_move =
                                vec![(sub_u, p_u as u8, p_v as u8), (sub_v, p_v as u8, p_w as u8)];
                        }
                    }
                }
            }
        }

        if best_score > info_gain_threshold {
            break;
        }

        for (sub, from_partition, to_partition) in partitions_to_move.drain(..) {
            move_sub_partition(
                state,
                sub_in_partition,
                sub_edge_cut_by_partition,
                sub,
                from_partition,
                to_partition,
            );
        }
    }

    fix_balance(
        state,
        sub_in_partition,
        sub_edge_cut_by_partition,
        refine_capacity,
        max_sub_in_partition,
    );
}

fn fix_balance<T>(
    state: &mut CuttanaState<T>,
    sub_in_partition: Vec<u64>,
    sub_edge_cut_by_partition: Vec<Vec<u64>>,
    refine_capacity: u64,
    max_sub_in_partition: u64,
) where
    T: Eq + Hash + Clone + Ord,
{
    loop {
        let mut best: Option<(f64, u16, u8, u8)> = None; // score, sub_partition, from_partition,
        // to_partition

        for (p_u, &p_u_sz) in state.global.partition_sizes.iter().enumerate() {
            for (p_v, &p_v_sz) in state.global.partition_sizes.iter().enumerate() {
                if p_u_sz as u64 <= refine_capacity
                    || p_v_sz as u64 >= refine_capacity
                    || sub_in_partition[p_v] >= max_sub_in_partition
                {
                    continue;
                }

                let (score, sub_u) = move_score[p_u][p_v].get_min();
                let sub_size = state.sub_partition(p_u as u8).partition_sizes[sub_u as usize];
                if (p_v_sz + sub_size) as u64 > refine_capacity {
                    continue;
                }

                match best {
                    None => {
                        best = Some((score, sub_u, p_u as u8, p_v as u8));
                    }
                    Some((best_score, _, _, _)) if score < best_score => {
                        best = Some((score, sub_u, p_u as u8, p_v as u8));
                    }
                    _ => {}
                }
            }
        }

        let Some((_, sub, from_partition, to_partition)) = best else {
            break;
        };
        move_sub_partition(
            state,
            sub_in_partition,
            sub_edge_cut_by_partition,
            sub,
            from_partition,
            to_partition,
        );
    }
}

fn move_sub_partition<T>(
    state: &mut CuttanaState<T>,
    sub_in_partition: Vec<u64>,
    sub_edge_cut_by_partition: Vec<Vec<u64>>,
    sub: u16,
    from_partition: u8,
    to_partition: u8,
) where
    T: Eq + Hash + Clone + Ord,
{
    for partition in 0..state.global.num_partitions {
        update_move_score(
            state,
            sub_edge_cut_by_partition,
            sub,
            partition,
            UpdateType::REMOVE,
        );
    }

    for adj_sub in state.sub_partition_graph[sub] {
        let edge_weight = state.sub_partition_graph[sub][adj_sub];
        sub_edge_cut_by_partition[adj_sub][from_partition] += edge_weight;
        sub_edge_cut_by_partition[adj_sub][to_partition] -= edge_weight;
    }

    state.global.partition_sizes[from_partition.into()] -=
        state.sub_partition(from_partition).partition_sizes[sub.into()];
    state.global.partition_sizes[to_partition.into()] +=
        state.sub_partition(from_partition).partition_sizes[sub.into()];

    sub_to_partition[sub] = to_partition;
    sub_in_partition[from_partition.into()] -= 1;
    sub_in_partition[to_partition.into()] += 1;

    let mut buckets: Vec<Vec<i32>> = Vec::with_capacity(state.global.num_partitions.into());
    for adj_sub in state.sub_partition_graph[sub] {
        buckets[sub_to_partition[adj_sub]].push(adj_sub);
    }

    // TODO: OMP PARALLEL
    for p in 0..state.global.num_partitions {
        for adj_sub in buckets[p.into()] {
            if sub_to_partition[adj_sub] == from_partition
                || sub_to_partition[adj_sub] == to_partition
            {
                for pp in 0..state.global.num_partitions {
                    update_move_score(
                        state,
                        sub_edge_cut_by_partition,
                        adj_sub,
                        pp,
                        UpdateType::UPDATE,
                    );
                }
            } else {
                update_move_score(
                    state,
                    sub_edge_cut_by_partition,
                    adj_sub,
                    from_partition,
                    UpdateType::UPDATE,
                );
                update_move_score(
                    state,
                    sub_edge_cut_by_partition,
                    adj_sub,
                    to_partition,
                    UpdateType::UPDATE,
                );
            }
        }
    }

    for pp in 0..state.global.num_partitions {
        update_move_score(state, sub_edge_cut_by_partition, sub, pp, UpdateType::ADD);
    }
}

fn update_move_score<T>(
    state: &mut CuttanaState<T>,
    sub_edge_cut_by_partition: Vec<Vec<u64>>,
    sub: u16,
    to_partition: u8,
    update: UpdateType,
) where
    T: Eq + Hash + Clone + Ord,
{
    let from_partition = sub_to_partition[sub];

    let edge_cut_after = sub_edge_cut_by_partition[sub.into()][to_partition];
    let edge_cut_before = sub_edge_cut_by_partition[sub.into()][from_partition];
    let delta = edge_cut_after - edge_cut_before;

    let st_pos = &mut sub_to_segtree_ind[sub][to_partition];
    match update {
        UpdateType::ADD => {
            *st_pos = move_score[from_partition][to_partition].add(sub, delta);
        }
        UpdateType::REMOVE => {
            move_score[from_partition][to_partition].remove(sub, st_pos);
            st_pos = -1;
        }
        UpdateType::UPDATE => {
            move_score[from_partition][to_partition].udpate(sub, delta, st_pos);
        }
    }
}
