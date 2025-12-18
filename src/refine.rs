use crate::state::CuttanaState;

/// A single atomic sub-partition move
#[derive(Debug, Clone)]
struct Move {
    sub: u16,
    from: u8,
    to: u8,
}

impl Move {
    #[inline]
    fn new(sub: u16, from: u8, to: u8) -> Self {
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
        let max_sub = (state.num_sub_partitions() as f64 / num_partitions * 1.5) as u64 + 1;

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
                let (score, sub) = (u64::MAX, 0u16); // placeholder for real scoring

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

        for from_idx in 0..state.num_partitions() {
            if self.partition_is_empty(state, from_idx) {
                continue;
            }
            for to_idx in 0..state.num_partitions() {
                if from_idx == to_idx || self.partition_at_sub_capacity(state, to_idx) {
                    continue;
                }

                let (from, to) = (from_idx as u8, to_idx as u8);
                let (score, sub) = (u64::MAX, 0u16); // placeholder

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
                for evict_idx in 0..state.num_partitions() {
                    if evict_idx == to_idx || self.partition_at_sub_capacity(state, evict_idx) {
                        continue;
                    }

                    let (score_2, sub_2) = (0u64, 0u16); // placeholder
                    let effective_score = score
                        + score_2
                        + state.sub_partitions[sub as usize].get_edge(sub_2)
                        + if from_idx == evict_idx {
                            state.sub_partitions[sub_2 as usize].get_edge(sub)
                        } else {
                            0
                        };

                    if best.as_ref().is_none_or(|b| effective_score < b.score) {
                        best = Some(MovePlan {
                            score: effective_score,
                            moves: vec![
                                Move::new(sub, from, to),
                                Move::new(sub_2, to, evict_idx as u8),
                            ],
                        });
                    }
                }
            }
        }

        best
    }

    fn partition_at_vertex_capacity<T>(&self, state: &CuttanaState<T>, p: usize) -> bool {
        state.global_assignments.partition_sizes[p] as u64 >= self.max_parent
    }

    fn partition_within_vertex_capacity<T>(&self, state: &CuttanaState<T>, p: usize) -> bool {
        state.global_assignments.partition_sizes[p] as u64 <= self.max_parent
    }

    fn partition_at_sub_capacity<T>(&self, state: &CuttanaState<T>, p: usize) -> bool {
        state.partitions[p].num_sub as u64 >= self.max_sub
    }

    fn partition_is_empty<T>(&self, state: &CuttanaState<T>, p: usize) -> bool {
        state.global_assignments.partition_sizes[p] == 0
    }

    fn sub_fits_in_partition<T>(
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

fn move_sub_partition<T>(state: &mut CuttanaState<T>, sub: u16, from: u8, to: u8) {
    update_move_score_all_partitions(state, sub, UpdateType::Remove);

    let (sub_idx, from_idx, to_idx) = (sub as usize, from as usize, to as usize);

    let edges: Vec<(usize, u64)> = state.sub_partitions[sub_idx]
        .edges
        .iter()
        .map(|(s, w)| (*s as usize, *w))
        .collect();

    // Update sub_edge_cut_by_partition using sub.edges
    for (adj_sub, edge_weight) in edges {
        let edge_cuts = &mut state.sub_partitions[adj_sub].edge_cuts;
        edge_cuts[to_idx] += edge_weight;
        edge_cuts[from_idx] -= edge_weight;
    }

    // Update partition sizes
    let sub_size = state.local_assignment_for(from).partition_sizes[sub_idx];
    state.global_assignments.partition_sizes[from_idx] -= sub_size;
    state.global_assignments.partition_sizes[to_idx] += sub_size;

    // Update assignment and counts
    state.sub_partitions[sub_idx].parent = to;
    state.partitions[from_idx].num_sub -= 1;
    state.partitions[to_idx].num_sub += 1;

    // Build buckets of neighbors grouped by parent
    let mut buckets = vec![Vec::<u16>::new(); state.num_partitions()];
    for &adj_sub in state.sub_partitions[sub_idx].edges.keys() {
        let parent = state.sub_partitions[adj_sub as usize].parent as usize;
        buckets[parent].push(adj_sub);
    }

    // Update move scores
    for bucket in &buckets {
        for &adj_sub in bucket {
            let adj_parent = state.sub_partitions[adj_sub as usize].parent;
            if adj_parent == from || adj_parent == to {
                update_move_score_all_partitions(state, adj_sub, UpdateType::Update);
            } else {
                update_move_score(state, adj_sub, from, UpdateType::Update);
                update_move_score(state, adj_sub, to, UpdateType::Update);
            }
        }
    }

    update_move_score_all_partitions(state, sub, UpdateType::Add);
}

fn update_move_score_all_partitions<T>(state: &mut CuttanaState<T>, sub: u16, update: UpdateType) {
    for partition in 0..state.num_partitions() {
        update_move_score(state, sub, partition as u8, update);
    }
}

fn update_move_score<T>(
    state: &mut CuttanaState<T>,
    sub: u16,
    adj_partition: u8,
    update: UpdateType,
) {
    let assigned_partition = state.sub_partitions[sub as usize].parent;
    let edge_cut = &state.sub_partitions[sub as usize].edge_cuts;
    let _delta = edge_cut[adj_partition as usize] - edge_cut[assigned_partition as usize];

    // TODO: Segment tree updates
    match update {
        UpdateType::Add => {}
        UpdateType::Remove => {}
        UpdateType::Update => {}
    }
}
