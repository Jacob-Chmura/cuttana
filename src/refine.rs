use crate::state::CuttanaState;
use core::f64;
use std::hash::Hash;

#[derive(Debug, Clone, Copy)]
enum UpdateType {
    Add,
    Remove,
    Update,
}

#[derive(Debug)]
struct SubPartitionMove {
    score: f64,
    sub: u16,
    from: u8,
    to: u8,
}

impl SubPartitionMove {
    #[inline]
    fn new(score: f64, sub: u16, from: u8, to: u8) -> Self {
        Self {
            score,
            sub,
            from,
            to,
        }
    }
}

pub(crate) fn refine<T>(state: &mut CuttanaState<T>, info_gain_threshold: f64, balance_slack: f64)
where
    T: Hash + Ord,
{
    let num_partitions = state.global.num_partitions as f64;
    let num_sub_partitions = state.sub_partition(0).num_partitions as f64;
    let num_vertices = state.global.metrics.vertex_count as f64;
    let refine_capacity = ((num_vertices / num_partitions) * (1.0 + balance_slack) + 1.0) as u64;
    let max_sub_in_partition = (num_sub_partitions / num_partitions * 1.5) as u64;

    // Initiliaze sub_edge_cut_by_partition from final sub_partition_graph
    for (sub, row) in state.sub_edge_cut_by_partition.iter_mut().enumerate() {
        let mut sub_total_edge_cut: u64 = 0;

        // subtract edge weights for the partition of each adjacent sub
        for (&adj_sub, &edge_weight) in state.sub_partition_graph[sub].iter() {
            let adj_part = state.sub_to_partition[adj_sub as usize] as usize;
            sub_total_edge_cut += edge_weight;
            row[adj_part] -= edge_weight;
        }

        // add total edge cut to all partitions
        for val in row.iter_mut() {
            *val += sub_total_edge_cut;
        }
    }

    fix_balance(state, refine_capacity, max_sub_in_partition);
    run_refinement(
        state,
        refine_capacity,
        max_sub_in_partition,
        info_gain_threshold,
    );
    fix_balance(state, refine_capacity, max_sub_in_partition);
}

fn run_refinement<T>(
    state: &mut CuttanaState<T>,
    refine_capacity: u64,
    max_sub_in_partition: u64,
    info_gain_threshold: f64,
) where
    T: Hash + Ord,
{
    let refine_capacity_ub = (refine_capacity as f64 * 1.1) as u64;

    loop {
        let mut moves: Option<(f64, Vec<SubPartitionMove>)> = None;

        for (from_idx, &from_size) in state.global.partition_sizes.iter().enumerate() {
            let from = from_idx as u8;
            let sub_part = &state.global_to_sub[&from];

            for (to_idx, &to_size) in state.global.partition_sizes.iter().enumerate() {
                if from_size == 0
                    || from_idx == to_idx
                    || state.sub_in_partition[to_idx] as u64 >= max_sub_in_partition
                {
                    continue;
                }

                let to = to_idx as u8;
                let (score, sub) = (f64::INFINITY, 0u16); // move_score[p_u][p_v].get_min()
                let sub_size = sub_part.partition_sizes[sub as usize];

                // Case 1: Direct move fits
                if (to_size + sub_size) as u64 <= refine_capacity_ub {
                    let candidate = (score, vec![SubPartitionMove::new(score, sub, from, to)]);
                    match &moves {
                        None => moves = Some(candidate),
                        Some((best_score, _)) if score < *best_score => moves = Some(candidate),
                        _ => {}
                    }
                    continue;
                }
                // Case 2: overflow, try secondary move (evict a sub partition out of to)
                else {
                    for (three_idx, &_) in state.global.partition_sizes.iter().enumerate() {
                        if to_idx == three_idx
                            || state.sub_in_partition[three_idx] as u64 >= max_sub_in_partition
                        {
                            continue;
                        }

                        let three = three_idx as u8;
                        let (score_2, sub_2) = (f64::INFINITY, 0u16); // move_score[p_v][p_w].get_min();
                        let mut score_3 = score + score_2;

                        score_3 += state.sub_partition_graph[sub as usize]
                            .get(&sub_2)
                            .copied()
                            .unwrap_or(0) as f64;

                        if from_idx == three_idx {
                            score_3 += state.sub_partition_graph[sub_2 as usize]
                                .get(&sub)
                                .copied()
                                .unwrap_or(0) as f64;
                        }

                        let candidate = (
                            score,
                            vec![
                                SubPartitionMove::new(score, sub, from, to),
                                SubPartitionMove::new(score_2, sub_2, to, three),
                            ],
                        );
                        match &moves {
                            None => moves = Some(candidate),
                            Some((best_score, _)) if score_3 < *best_score => {
                                moves = Some(candidate)
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        let (_, moves) = match moves {
            Some((score, moves)) if score <= info_gain_threshold => (score, moves),
            _ => break,
        };

        for m in moves {
            move_sub_partition(state, &m)
        }
    }
}

fn fix_balance<T>(state: &mut CuttanaState<T>, refine_capacity: u64, max_sub_in_partition: u64)
where
    T: Hash + Ord,
{
    loop {
        let mut best_move: Option<SubPartitionMove> = None;

        for (from_idx, &from_size) in state.global.partition_sizes.iter().enumerate() {
            let from = from_idx as u8;
            let sub_part = &state.global_to_sub[&from];

            for (to_idx, &to_size) in state.global.partition_sizes.iter().enumerate() {
                if from_size as u64 <= refine_capacity
                    || to_size as u64 >= refine_capacity
                    || state.sub_in_partition[to_idx] as u64 >= max_sub_in_partition
                {
                    continue;
                }

                let to = to_idx as u8;
                let (score, sub) = (f64::INFINITY, 0u16); // move_score[p_u][p_v].get_min();
                let sub_size = sub_part.partition_sizes[sub as usize];
                if (to_size + sub_size) as u64 > refine_capacity {
                    continue;
                }

                let candidate = SubPartitionMove::new(score, sub, from, to);

                match &best_move {
                    None => best_move = Some(candidate),
                    Some(b) if candidate.score < b.score => best_move = Some(candidate),
                    _ => {}
                }
            }
        }

        let Some(best_move) = best_move else {
            break;
        };

        move_sub_partition(state, &best_move);
    }
}

fn move_sub_partition<T>(state: &mut CuttanaState<T>, m: &SubPartitionMove)
where
    T: Hash + Ord,
{
    update_move_score_all_partitions(state, m.sub, UpdateType::Remove);

    let (sub, from, to) = (m.sub as usize, m.from as usize, m.to as usize);
    for (&adj, &edge_weight) in state.sub_partition_graph[sub].iter() {
        state.sub_edge_cut_by_partition[adj as usize][to] += edge_weight;
        state.sub_edge_cut_by_partition[adj as usize][from] -= edge_weight;
    }

    let sub_size = state.sub_partition(m.from).partition_sizes[sub];
    state.global.partition_sizes[from] -= sub_size;
    state.global.partition_sizes[to] += sub_size;
    state.sub_to_partition[sub] = m.to;
    state.sub_in_partition[from] -= 1;
    state.sub_in_partition[to] += 1;

    let mut buckets = vec![Vec::<u16>::new(); state.global.num_partitions as usize];
    for &adj in state.sub_partition_graph[sub].keys() {
        let p = state.sub_to_partition[adj as usize] as usize;
        buckets[p].push(adj);
    }

    // TODO: OMP PARALLEL
    for bucket in &buckets {
        for &adj in bucket {
            let adj_part = state.sub_to_partition[adj as usize];
            if adj_part == m.from || adj_part == m.to {
                update_move_score_all_partitions(state, adj, UpdateType::Update);
            } else {
                update_move_score(state, adj, m.from, UpdateType::Update);
                update_move_score(state, adj, m.to, UpdateType::Update);
            }
        }
    }
    update_move_score_all_partitions(state, m.sub, UpdateType::Add);
}

fn update_move_score_all_partitions<T>(state: &mut CuttanaState<T>, sub: u16, update: UpdateType) {
    for partition in 0..state.global.num_partitions {
        update_move_score(state, sub, partition, update);
    }
}

fn update_move_score<T>(
    state: &mut CuttanaState<T>,
    sub: u16,
    adj_partition: u8,
    update: UpdateType,
) {
    let assigned_partition = state.sub_to_partition[sub as usize];
    let edge_cut = &state.sub_edge_cut_by_partition[sub as usize];
    let _delta = edge_cut[adj_partition as usize] - edge_cut[assigned_partition as usize];
    //let st_pos = &mut sub_to_segtree_ind[sub][adj_partition];
    match update {
        UpdateType::Add => {
            //*st_pos = move_score[assigned_partition][adj_partition.into()].add(sub, delta);
        }
        UpdateType::Remove => {
            //move_score[assigned_partition][adj_partition.into()].remove(sub, st_pos);
            //st_pos = -1;
        }
        UpdateType::Update => {
            //move_score[assigned_partition][adj_partition.into()].update(sub, delta, st_pos);
        }
    }
}
