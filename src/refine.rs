use crate::state::{CuttanaState, GlobalSubPartitionId, LocalSubPartitionId, PartitionId};

/// A single atomic sub-partition move
#[derive(Debug, Clone)]
struct Move {
    sub: usize,
    from: usize,
    to: usize,
}

impl Move {
    #[inline]
    fn new(sub: usize, from: usize, to: usize) -> Self {
        Self { sub, from, to }
    }
}

/// A candidate plan consisting of one or more moves with an associated score
#[derive(Debug)]
struct MovePlan {
    score: u64,
    moves: Vec<Move>,
}

#[derive(Debug, Clone, Copy)]
enum UpdateType {
    Add,
    Remove,
    Update,
}

pub(crate) struct Refiner {
    gain_threshold: u64,
    max_parent: u64,
    max_sub: u64,
    refine_capacity: u64,
}

impl Refiner {
    pub fn new<T>(state: &mut CuttanaState<T>, balance_slack: f64, gain_threshold: u64) -> Self {
        state.compute_sub_partition_edge_cuts();

        let num_vertices = state.global_assignments.metrics.vertex_count as f64;
        let num_partitions = state.num_partitions() as f64;
        let max_parent = (num_vertices / num_partitions * (1.0 + balance_slack)) as u64 + 1;
        let max_sub = (state.total_num_sub_partitions() as f64 / num_partitions * 1.5) as u64 + 1;

        Self {
            gain_threshold,
            max_parent,
            max_sub,
            refine_capacity: (max_parent as f64 * 1.1) as u64,
        }
    }

    pub fn fix_balance<T>(&self, state: &mut CuttanaState<T>) {
        while let Some(plan) = self.find_best_balance_plan(state) {
            for m in plan.moves {
                move_sub_partition(state, m.sub, m.from, m.to);
            }
        }
    }

    pub fn refine<T>(&self, state: &mut CuttanaState<T>) {
        while let Some(plan) = self.find_best_refine_plan(state) {
            if plan.score > self.gain_threshold {
                break;
            }
            for m in plan.moves {
                move_sub_partition(state, m.sub, m.from, m.to);
            }
        }
    }

    fn find_best_balance_plan<T>(&self, state: &CuttanaState<T>) -> Option<MovePlan> {
        let mut best: Option<MovePlan> = None;

        for from in 0..state.num_partitions() {
            if self.partition_within_vertex_capacity(state, from) {
                continue;
            }
            for to in 0..state.num_partitions() {
                if self.partition_at_vertex_capacity(state, to)
                    || self.partition_at_sub_capacity(state, to)
                {
                    continue;
                }
                let (score, sub) = (u64::MAX, 0usize); // placeholder for real scoring

                if self.sub_fits_in_partition(state, from, sub, to, self.max_parent)
                    && best.as_ref().is_none_or(|b| score < b.score)
                {
                    best = Some(MovePlan {
                        score,
                        moves: vec![Move::new(sub, from, to)],
                    });
                }
            }
        }

        best
    }

    fn find_best_refine_plan<T>(&self, state: &CuttanaState<T>) -> Option<MovePlan> {
        let mut best: Option<MovePlan> = None;

        for from in 0..state.num_partitions() {
            if self.partition_is_empty(state, from) {
                continue;
            }
            for to in 0..state.num_partitions() {
                if from == to || self.partition_at_sub_capacity(state, to) {
                    continue;
                }
                let (score, sub) = (u64::MAX, 0usize); // placeholder

                // Case 1: direct move
                if self.sub_fits_in_partition(state, from, sub, to, self.refine_capacity) {
                    if best.as_ref().is_none_or(|b| score < b.score) {
                        best = Some(MovePlan {
                            score,
                            moves: vec![Move::new(sub, from, to)],
                        });
                    }
                    continue;
                }

                // Case 2: eviction
                for evict in 0..state.num_partitions() {
                    if evict == to || self.partition_at_sub_capacity(state, evict) {
                        continue;
                    }
                    let (score_2, sub_2) = (0u64, 0usize); // placeholder
                    // TODO: global-local map
                    let sub_global: GlobalSubPartitionId =
                        from * state.num_sub_partitions_per_partition() + sub;
                    let sub_2_global: GlobalSubPartitionId =
                        to * state.num_sub_partitions_per_partition() + sub_2;

                    let effective_score = score
                        + score_2
                        + state.sub_partitions[sub_global].get_edge(sub_2_global)
                        + if from == evict {
                            state.sub_partitions[sub_2_global].get_edge(sub_global)
                        } else {
                            0
                        };

                    if best.as_ref().is_none_or(|b| effective_score < b.score) {
                        best = Some(MovePlan {
                            score: effective_score,
                            moves: vec![Move::new(sub, from, to), Move::new(sub_2, to, evict)],
                        });
                    }
                }
            }
        }

        best
    }

    fn partition_at_vertex_capacity<T>(&self, state: &CuttanaState<T>, p: PartitionId) -> bool {
        state.global_assignments.partition_sizes[p] as u64 >= self.max_parent
    }

    fn partition_within_vertex_capacity<T>(&self, state: &CuttanaState<T>, p: PartitionId) -> bool {
        state.global_assignments.partition_sizes[p] as u64 <= self.max_parent
    }

    fn partition_at_sub_capacity<T>(&self, state: &CuttanaState<T>, p: PartitionId) -> bool {
        state.partitions[p].num_sub as u64 >= self.max_sub
    }

    fn partition_is_empty<T>(&self, state: &CuttanaState<T>, p: PartitionId) -> bool {
        state.global_assignments.partition_sizes[p] == 0
    }

    fn sub_fits_in_partition<T>(
        &self,
        state: &CuttanaState<T>,
        parent: PartitionId,
        sub: LocalSubPartitionId,
        target: PartitionId,
        limit: u64,
    ) -> bool {
        let sub_size = state.local_assignments[&parent].partition_sizes[sub];
        let to_size = state.global_assignments.partition_sizes[target];
        (to_size + sub_size) as u64 <= limit
    }
}

fn move_sub_partition<T>(
    state: &mut CuttanaState<T>,
    sub: LocalSubPartitionId,
    from: PartitionId,
    to: PartitionId,
) {
    let sub_global: GlobalSubPartitionId = from * state.num_sub_partitions_per_partition() + sub;
    update_move_score_all_partitions(state, sub_global, UpdateType::Remove);

    let edges: Vec<(GlobalSubPartitionId, u64)> = state.sub_partitions[sub_global]
        .edges
        .iter()
        .map(|(s, w)| (*s, *w))
        .collect();

    // Update sub_edge_cut_by_partition using sub.edges
    for (adj_sub_global, edge_weight) in edges {
        let edge_cuts = &mut state.sub_partitions[adj_sub_global].edge_cuts;
        edge_cuts[to] += edge_weight;
        edge_cuts[from] -= edge_weight;
    }

    // Update partition sizes
    let sub_size = state.local_assignment_for(from).partition_sizes[sub];
    state.global_assignments.partition_sizes[from] -= sub_size;
    state.global_assignments.partition_sizes[to] += sub_size;

    // Update assignment and counts
    state.sub_partitions[sub_global].parent = to;
    state.partitions[from].num_sub -= 1;
    state.partitions[to].num_sub += 1;

    // Build buckets of neighbors grouped by parent
    let mut buckets = vec![Vec::new(); state.num_partitions()];
    for &adj_sub_global in state.sub_partitions[sub_global].edges.keys() {
        let parent = state.sub_partitions[adj_sub_global].parent;
        buckets[parent].push(adj_sub_global);
    }

    // Update move scores
    for bucket in &buckets {
        for &adj_sub_global in bucket {
            let adj_parent = state.sub_partitions[adj_sub_global].parent;
            if adj_parent == from || adj_parent == to {
                update_move_score_all_partitions(state, adj_sub_global, UpdateType::Update);
            } else {
                update_move_score(state, adj_sub_global, from, UpdateType::Update);
                update_move_score(state, adj_sub_global, to, UpdateType::Update);
            }
        }
    }

    update_move_score_all_partitions(state, sub_global, UpdateType::Add);
}

fn update_move_score_all_partitions<T>(
    state: &mut CuttanaState<T>,
    sub_global: GlobalSubPartitionId,
    update: UpdateType,
) {
    for partition in 0..state.num_partitions() {
        update_move_score(state, sub_global, partition, update);
    }
}

fn update_move_score<T>(
    state: &mut CuttanaState<T>,
    sub_global: GlobalSubPartitionId,
    adj_partition: PartitionId,
    update: UpdateType,
) {
    let assigned_partition = state.sub_partitions[sub_global].parent;
    let edge_cut = &state.sub_partitions[sub_global].edge_cuts;
    let _delta = edge_cut[adj_partition] - edge_cut[assigned_partition];

    // TODO: Segment tree updates
    match update {
        UpdateType::Add => {}
        UpdateType::Remove => {}
        UpdateType::Update => {}
    }
}
