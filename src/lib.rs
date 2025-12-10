use core::f64;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::hash::Hash;

/// Tracks vertex-based partion assignment output from a graph partioner
struct PartitionState<T> {
    assignments: HashMap<T, usize>,
    partition_sizes: Vec<usize>,
}

impl<T> PartitionState<T> {
    pub fn new(num_partitions: usize) -> Self {
        Self {
            assignments: HashMap::new(),
            partition_sizes: vec![0, num_partitions],
        }
    }

    /// Normalized number of edges whose endpoints are in different partions
    pub fn edge_cut_cost(&self) -> f64 {
        unimplemented!();
    }

    /// Normalized cost of cross-partion neighbors over the entire graph
    pub fn communication_volume_cost(&self) -> f64 {
        unimplemented!();
    }

    pub fn assign(&mut self, v: T, partition: usize)
    where
        T: Eq + Hash,
    {
        self.assignments.insert(v, partition);
        self.partition_sizes[partition] += 1;
    }
}

/// Manages buffered vertices which are not ready to partition
struct BufferManager<T>
where
    T: Eq + Clone + Hash,
{
    heap: BinaryHeap<BufferEntry<T>>,
    map: HashMap<T, BufferEntry<T>>,
    capacity: usize,
}

impl<T> BufferManager<T>
where
    T: Eq + Clone + Hash,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            heap: BinaryHeap::new(),
            map: HashMap::new(),
            capacity,
        }
    }

    pub fn insert(&mut self, v: T, nbrs: &Vec<T>) {
        let entry = BufferEntry {
            score: compute_buffer_score(v.clone(), &nbrs),
            vertex: v.clone(),
            nbrs: nbrs.clone(),
        };
        self.heap.push(entry.clone());
        self.map.insert(v, entry);
    }

    pub fn is_at_capacity(&self) -> bool {
        self.heap.len() >= self.capacity
    }

    pub fn evict(&mut self) -> Option<(T, Vec<T>)> {
        if let Some(entry) = self.heap.pop() {
            self.map.remove(&entry.vertex);
            return Some((entry.vertex, entry.nbrs));
        }
        None
    }

    pub fn update_score(&mut self, v: T) {
        if let Some(mut entry) = self.map.remove(&v) {
            entry.score += 2f64 / entry.nbrs.len() as f64;
            self.heap.push(entry.clone()); // TODO: old one must be removed
            self.map.insert(v.clone(), entry);
        }
    }
}

#[derive(Clone, PartialEq)]
struct BufferEntry<T>
where
    T: Eq + Hash + Clone,
{
    score: f64,
    vertex: T,
    nbrs: Vec<T>,
}

impl<T> Eq for BufferEntry<T> where T: Eq + Hash + Clone {}

impl<T> Ord for BufferEntry<T>
where
    T: Eq + Hash + Clone,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(Ordering::Equal)
    }
}

impl<T> PartialOrd for BufferEntry<T>
where
    T: Eq + Hash + Clone,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A pull-based vertex-stream. Consumers call `next_vertex()` until None
pub trait VertexStream {
    type VertexID;

    fn next_vertex(&mut self) -> Option<(Self::VertexID, Vec<Self::VertexID>)>;
}

pub struct AdjacencyList<T> {
    data: Vec<(T, Vec<T>)>,
    pos: usize,
}

impl<T> AdjacencyList<T> {
    pub fn new(data: Vec<(T, Vec<T>)>) -> Self {
        Self { data, pos: 0 }
    }

    // TODO: Validate adjacency list has no duplicate vertices
}

impl<T: Copy> VertexStream for AdjacencyList<T> {
    type VertexID = T;

    fn next_vertex(&mut self) -> Option<(Self::VertexID, Vec<Self::VertexID>)> {
        let out = self.data.get(self.pos)?.clone();
        self.pos += 1;
        Some(out)
    }
}

impl<T: Copy> Iterator for AdjacencyList<T> {
    type Item = (T, Vec<T>);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_vertex()
    }
}

pub fn cuttana_partition<T>(
    mut stream: impl VertexStream<VertexID = T>,
    num_partitions: usize,
    max_partition_size: usize,
    max_buffer_size: usize,
    degree_max: usize,
) -> PartitionState<T>
where
    T: Eq + Hash + Clone + Default,
{
    let mut buffer = BufferManager::<T>::new(max_buffer_size);
    let mut state = PartitionState::<T>::new(num_partitions);

    while let Some((v, nbrs)) = stream.next_vertex() {
        if nbrs.len() >= degree_max {
            partition_vertex(
                v.clone(),
                &nbrs,
                &mut state,
                &mut buffer,
                max_partition_size,
            );
        } else {
            buffer.insert(v, &nbrs);
        }

        if buffer.is_at_capacity()
            && let Some((v, nbrs)) = buffer.evict()
        {
            partition_vertex(v, &nbrs, &mut state, &mut buffer, max_partition_size);
        }
    }

    while let Some((v, nbrs)) = buffer.evict() {
        partition_vertex(v, &nbrs, &mut state, &mut buffer, max_partition_size);
    }

    state
}

fn compute_buffer_score<T>(v: T, nbrs: &Vec<T>) -> f64 {
    // TODO: custimze this: 2 * cnt_ajd_partitioned / nbrs.len() + nbr.len() / buffer_deg_threshold
    0.0
}

fn compute_balance_score(partition: usize) -> f64 {
    // alpha = num_partition **(gamma - 1) * edge_count / vertex_count ** gamma
    //TODO: customize this: return alpha * gamma * state.partition_sizes[partition] ** (gamma - 1)
    0.0
}

fn partition_vertex<T>(
    v: T,
    nbrs: &Vec<T>,
    state: &mut PartitionState<T>,
    buffer: &mut BufferManager<T>,
    max_partition_size: usize,
) where
    T: Eq + Hash + Clone + Default,
{
    // find best partition among the K partitions
    let (mut best_score, mut best_partition, mut tie_count) = (f64::NEG_INFINITY, 0, 0);

    let mut update_best_partition = |partition: usize, score: f64| {
        if score > best_score {
            (best_score, best_partition, tie_count) = (score, partition, 1);
        } else if score == best_score {
            tie_count += 1;
            // TODO: uniform integer sample [1, tie_count] inclusive with rng
            best_partition = partition;
        }
    };

    let nbrs_per_partition = vec![0, state.partition_sizes.len()];
    for nbr in nbrs {
        if let Some(&partition) = state.assignments.get(nbr)
            && state.partition_sizes[partition] < max_partition_size
        {
            let nbr_count = nbrs_per_partition[partition] + 1;
            let score = nbr_count as f64 - compute_balance_score(partition);
            update_best_partition(partition, score);
        }
    }

    if let Some((partition, _)) = state
        .partition_sizes
        .iter()
        .enumerate()
        .min_by_key(|&(_, sz)| sz)
    {
        update_best_partition(partition, -compute_balance_score(partition));
    }

    state.assign(v.clone(), best_partition);

    for nbr in nbrs {
        buffer.update_score(nbr.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iterate_adj_list() {
        let data = vec![(0, vec![1, 2]), (1, vec![0]), (2, vec![0])];
        let mut stream = AdjacencyList::new(data.clone());

        let mut seen = vec![];
        for (v, nbrs) in &mut stream {
            seen.push((v, nbrs));
        }
        assert_eq!(seen, data);
    }

    #[test]
    fn test_empty_adj_list() {
        let mut stream: AdjacencyList<u32> = AdjacencyList::new(vec![]);
        assert!(stream.next_vertex().is_none());
    }

    #[test]
    fn test_cuttana() {
        let data = vec![(0, vec![1, 2]), (1, vec![0]), (2, vec![0])];
        let stream = AdjacencyList::new(data.clone());
        const NUM_PARTITIONS: usize = 16;
        const MAX_PARTITION_SIZE: usize = 1024;
        const MAX_BUFFER_SIZE: usize = 1_000_000;
        const DEGREE_MAX: usize = 1000;

        let _p: PartitionState<i32> = cuttana_partition(
            stream,
            NUM_PARTITIONS,
            MAX_PARTITION_SIZE,
            MAX_BUFFER_SIZE,
            DEGREE_MAX,
        );
    }
}
