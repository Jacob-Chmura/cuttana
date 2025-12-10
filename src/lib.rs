use core::f64;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::hash::Hash;

/// Tracks vertex-based partion assignment output from a graph partioner
#[derive(Default)]
pub struct PartitionState<T> {
    assignments: HashMap<T, usize>,
    partition_sizes: Vec<usize>,
}

impl<T> PartitionState<T> {
    /// Normalized number of edges whose endpoints are in different partions
    pub fn edge_cut_cost(&self) -> f64 {
        unimplemented!();
    }

    /// Normalized cost of cross-partion neighbors over the entire graph
    pub fn communication_volume_cost(&self) -> f64 {
        unimplemented!();
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
    let mut buffer_heap = BinaryHeap::<BufferEntry<T>>::new();
    let mut buffer_map = HashMap::<T, BufferEntry<T>>::new();

    let mut state = PartitionState::<T>::default();
    state.partition_sizes = vec![0, num_partitions];

    while let Some((v, nbrs)) = stream.next_vertex() {
        if nbrs.len() >= degree_max {
            partition_vertex(
                v.clone(),
                &nbrs,
                &mut state,
                &mut buffer_heap,
                &mut buffer_map,
                max_partition_size,
            );
        } else {
            let entry = BufferEntry {
                score: compute_buffer_score(v.clone(), &nbrs),
                vertex: v.clone(),
                nbrs: nbrs.clone(),
            };
            buffer_heap.push(entry.clone());
            buffer_map.insert(v, entry);
        }

        if buffer_heap.len() > max_buffer_size
            && let Some(entry) = buffer_heap.pop()
        {
            buffer_map.remove(&entry.vertex);
            partition_vertex(
                entry.vertex,
                &entry.nbrs,
                &mut state,
                &mut buffer_heap,
                &mut buffer_map,
                max_partition_size,
            );
        }
    }

    while !buffer_heap.is_empty() {
        let entry = buffer_heap.pop().unwrap();
        buffer_map.remove(&entry.vertex);

        partition_vertex(
            entry.vertex,
            &entry.nbrs,
            &mut state,
            &mut buffer_heap,
            &mut buffer_map,
            max_partition_size,
        );
    }

    state
}

fn compute_buffer_score<T>(v: T, nbrs: &Vec<T>) -> f64 {
    // 2 * cnt_ajd_partitioned / nbrs.len() + nbr.len() / buffer_deg_threshold
    0.0
}

fn compute_balance_score(partition: usize) -> f64 {
    // alpha = num_partition **(gamma - 1) * edge_count / vertex_count ** gamma
    //return alpha * gamma * state.partition_sizes[partition] ** (gamma - 1)
    0.0
}

fn partition_vertex<T>(
    v: T,
    nbrs: &Vec<T>,
    state: &mut PartitionState<T>,
    buffer_heap: &mut BinaryHeap<BufferEntry<T>>,
    buffer_map: &mut HashMap<T, BufferEntry<T>>,
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

    for nbr in nbrs {
        if let Some(&partition) = state.assignments.get(nbr)
            && state.partition_sizes[partition] < max_partition_size
        {
            /*
            nbr_count, last_v = part_nbr[p];
            if(last_v != v) {nbr_count = 0, last_v = v};
            nbr_count += 1;
            */
            let nbr_count = 1;
            let score = nbr_count as f64 - compute_balance_score(partition);
            update_best_partition(partition, score);
        }
    }

    if let Some(partition) = state
        .partition_sizes
        .iter()
        .enumerate()
        .min_by_key(|&(_, sz)| sz)
        .map(|(i, _)| i)
    {
        update_best_partition(partition, -compute_balance_score(partition));
    }

    state.assignments.insert(v, best_partition);
    state.partition_sizes[best_partition] += 1;

    for nbr in nbrs {
        if let Some(mut entry) = buffer_map.remove(&nbr) {
            entry.score += 2f64 / entry.nbrs.len() as f64;
            buffer_heap.push(entry.clone()); // TODO: old ones must be removed or skipped
            buffer_map.insert(nbr.clone(), entry);
        }
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
