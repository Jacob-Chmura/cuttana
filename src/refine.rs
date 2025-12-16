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

    let total_sub_partitions =
        state.sub_partition(0).num_partitions as u64 * state.global.num_partitions as u64;
    let mut sub_to_partition: Vec<u8> = (0..total_sub_partitions)
        .map(|i| (i / state.sub_partition(0).num_partitions as u64) as u8)
        .collect();

    // TODO: Create segment trees
    let mut move_score =
        vec![vec![0; state.global.num_partitions.into()]; state.global.num_partitions.into()];

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
        &mut move_score,
        &mut sub_in_partition,
        &mut sub_to_partition,
        &mut sub_edge_cut_by_partition,
        refine_capacity,
        max_sub_in_partition,
    );

    loop {
        let mut best_score = f64::INFINITY;
        let mut partitions_to_move: Vec<(u16, u8, u8)> = Vec::new(); // sub, assigned_partition,
        // adj_partition

        for (p_u, &p_u_sz) in state.global.partition_sizes.iter().enumerate() {
            for (p_v, &p_v_sz) in state.global.partition_sizes.iter().enumerate() {
                if p_u_sz == 0 || p_u == p_v || sub_in_partition[p_v] >= max_sub_in_partition {
                    continue;
                }

                // let (score, sub_u) = move_score[p_u][p_v].get_min();
                let (score, sub_u) = (f64::INFINITY, 0);
                let sub_u_size = state
                    .global_to_sub
                    .get(&(p_u as u8))
                    .unwrap()
                    .partition_sizes[sub_u as usize];

                // Case 1: direct move fits
                if (p_v_sz + sub_u_size) as u64 <= refine_capacity_ub {
                    if (score as f64) < best_score {
                        best_score = score;
                        partitions_to_move = vec![(sub_u, p_u as u8, p_v as u8)];
                    }
                }
                // Case 2: overflow: try secondary move p_v -> p_w
                else {
                    for (p_w, _) in state.global.partition_sizes.iter().enumerate() {
                        if p_w == p_v || sub_in_partition[p_w] >= max_sub_in_partition {
                            continue;
                        }

                        //let (score_2, sub_v) = move_score[p_v][p_w].get_min();
                        let (score_2, sub_v) = (f64::INFINITY, 1);
                        let mut effective_score = score + score_2;

                        effective_score += state.sub_partition_graph[sub_u as usize]
                            .get(&sub_v)
                            .copied()
                            .unwrap_or(0) as f64;

                        if p_w == p_u {
                            effective_score += state.sub_partition_graph[sub_v as usize]
                                .get(&sub_u)
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

        for (sub, assigned_partition, adj_partition) in partitions_to_move.drain(..) {
            move_sub_partition(
                state,
                &mut move_score,
                &mut sub_in_partition,
                &mut sub_to_partition,
                &mut sub_edge_cut_by_partition,
                sub,
                assigned_partition,
                adj_partition,
            );
        }
    }

    fix_balance(
        state,
        &mut move_score,
        &mut sub_in_partition,
        &mut sub_to_partition,
        &mut sub_edge_cut_by_partition,
        refine_capacity,
        max_sub_in_partition,
    );
}

fn fix_balance<T>(
    state: &mut CuttanaState<T>,
    move_score: &mut Vec<Vec<i32>>,
    sub_in_partition: &mut Vec<u64>,
    sub_to_partition: &mut Vec<u8>,
    sub_edge_cut_by_partition: &mut Vec<Vec<u64>>,
    refine_capacity: u64,
    max_sub_in_partition: u64,
) where
    T: Eq + Hash + Clone + Ord,
{
    loop {
        let mut best: Option<(f64, u16, u8, u8)> = None; // score, sub_partition, assigned_partition,
        // adj_partition

        for (p_u, &p_u_sz) in state.global.partition_sizes.iter().enumerate() {
            for (p_v, &p_v_sz) in state.global.partition_sizes.iter().enumerate() {
                if p_u_sz as u64 <= refine_capacity
                    || p_v_sz as u64 >= refine_capacity
                    || sub_in_partition[p_v] >= max_sub_in_partition
                {
                    continue;
                }

                //let (score, sub_u) = move_score[p_u][p_v].get_min();
                let (score, sub_u) = (f64::INFINITY, 0);
                let sub_size = state
                    .global_to_sub
                    .get(&(p_u as u8))
                    .unwrap()
                    .partition_sizes[sub_u as usize];
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

        let Some((_, sub, assigned_partition, adj_partition)) = best else {
            break;
        };
        move_sub_partition(
            state,
            move_score,
            sub_in_partition,
            sub_to_partition,
            sub_edge_cut_by_partition,
            sub,
            assigned_partition,
            adj_partition,
        );
    }
}

fn move_sub_partition<T>(
    state: &mut CuttanaState<T>,
    move_score: &mut Vec<Vec<i32>>,
    sub_in_partition: &mut Vec<u64>,
    sub_to_partition: &mut Vec<u8>,
    sub_edge_cut_by_partition: &mut Vec<Vec<u64>>,
    sub: u16,
    assigned_partition: u8,
    adj_partition: u8,
) where
    T: Eq + Hash + Clone + Ord,
{
    for partition in 0..state.global.num_partitions {
        update_move_score(
            state,
            move_score,
            sub_edge_cut_by_partition,
            sub_to_partition,
            sub,
            partition,
            UpdateType::REMOVE,
        );
    }

    for (&adj_sub, edge_weight) in state.sub_partition_graph[sub as usize].iter() {
        sub_edge_cut_by_partition[adj_sub as usize][assigned_partition as usize] += edge_weight;
        sub_edge_cut_by_partition[adj_sub as usize][adj_partition as usize] -= edge_weight;
    }

    let sub_partition_size = state.sub_partition(assigned_partition).partition_sizes[sub as usize];
    state.global.partition_sizes[assigned_partition as usize] -= sub_partition_size;
    state.global.partition_sizes[adj_partition as usize] += sub_partition_size;

    sub_to_partition[sub as usize] = adj_partition;
    sub_in_partition[assigned_partition as usize] -= 1;
    sub_in_partition[adj_partition as usize] += 1;

    let mut buckets: Vec<Vec<u16>> = Vec::with_capacity(state.global.num_partitions.into());
    for &adj_sub in state.sub_partition_graph[sub as usize].keys() {
        buckets[sub_to_partition[adj_sub as usize] as usize].push(adj_sub);
    }

    // TODO: OMP PARALLEL
    for p in 0..state.global.num_partitions {
        for adj_sub in buckets[p as usize].clone() {
            if sub_to_partition[adj_sub as usize] == assigned_partition
                || sub_to_partition[adj_sub as usize] == adj_partition
            {
                for pp in 0..state.global.num_partitions {
                    update_move_score(
                        state,
                        move_score,
                        sub_edge_cut_by_partition,
                        sub_to_partition,
                        adj_sub,
                        pp,
                        UpdateType::UPDATE,
                    );
                }
            } else {
                update_move_score(
                    state,
                    move_score,
                    sub_edge_cut_by_partition,
                    sub_to_partition,
                    adj_sub as u16,
                    assigned_partition,
                    UpdateType::UPDATE,
                );
                update_move_score(
                    state,
                    move_score,
                    sub_edge_cut_by_partition,
                    sub_to_partition,
                    adj_sub as u16,
                    adj_partition,
                    UpdateType::UPDATE,
                );
            }
        }
    }

    for partition in 0..state.global.num_partitions {
        update_move_score(
            state,
            move_score,
            sub_edge_cut_by_partition,
            sub_to_partition,
            sub,
            partition,
            UpdateType::ADD,
        );
    }
}

fn update_move_score<T>(
    _state: &mut CuttanaState<T>,
    _move_score: &mut Vec<Vec<i32>>,
    sub_edge_cut_by_partition: &Vec<Vec<u64>>,
    sub_to_partition: &Vec<u8>,
    sub: u16,
    adj_partition: u8,
    update: UpdateType,
) where
    T: Eq + Hash + Clone + Ord,
{
    let _assigned_partition = sub_to_partition[sub as usize];
    let _edge_cut_after = sub_edge_cut_by_partition[sub as usize][adj_partition as usize];
    let _edge_cut_before = sub_edge_cut_by_partition[sub as usize][_assigned_partition as usize];
    let _delta = _edge_cut_after - _edge_cut_before;

    //let st_pos = &mut sub_to_segtree_ind[sub][adj_partition];
    match update {
        UpdateType::ADD => {
            //*st_pos = move_score[assigned_partition][adj_partition.into()].add(sub, delta);
        }
        UpdateType::REMOVE => {
            //move_score[assigned_partition][adj_partition.into()].remove(sub, st_pos);
            //st_pos = -1;
        }
        UpdateType::UPDATE => {
            //move_score[assigned_partition][adj_partition.into()].update(sub, delta, st_pos);
        }
    }
}
