use crate::partition::PartitionState;
use core::f64;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::hash::Hash;

/// Manages buffered vertices which are not ready to partition
pub(crate) struct BufferManager<T, S>
where
    T: Eq + Clone + Hash,
    S: BufferScorer,
{
    heap: BinaryHeap<BufferEntry<T>>,
    map: HashMap<T, BufferEntry<T>>,
    capacity: usize,
    scorer: S,
}

impl<T, S> BufferManager<T, S>
where
    T: Eq + Clone + Hash,
    S: BufferScorer,
{
    pub fn new(capacity: usize, scorer: S) -> Self {
        Self {
            heap: BinaryHeap::new(),
            map: HashMap::new(),
            capacity,
            scorer,
        }
    }

    pub fn insert(&mut self, v: &T, nbrs: &[T], state: &PartitionState<T>) {
        let entry = BufferEntry {
            score: self.scorer.score(v, nbrs, state),
            vertex: v.clone(),
            nbrs: nbrs.to_vec(),
        };
        self.heap.push(entry.clone());
        self.map.insert(v.clone(), entry);
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

    pub fn update_score(&mut self, v: &T) {
        if let Some(mut entry) = self.map.remove(v) {
            entry.score += 2.0 / entry.nbrs.len() as f64;
            self.heap.push(entry.clone()); // TODO: old one must be removed
            self.map.insert(v.clone(), entry);
        }
    }
}

#[derive(Clone, PartialEq)]
pub(crate) struct BufferEntry<T>
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

pub trait BufferScorer {
    fn score<T: Eq + Hash + Clone>(&self, v: &T, nbrs: &[T], state: &PartitionState<T>) -> f64;
}

pub struct CuttanaBufferScorer {
    theta: f64,
    buffer_deg_threshold: f64,
}

impl CuttanaBufferScorer {
    pub fn new(theta: f64, buffer_deg_threshold: f64) -> Self {
        Self {
            theta,
            buffer_deg_threshold,
        }
    }
}

impl BufferScorer for CuttanaBufferScorer {
    fn score<T: Eq + Hash + Clone>(&self, _v: &T, nbrs: &[T], state: &PartitionState<T>) -> f64 {
        let degree = nbrs.len() as f64;
        let num_nbrs_partitioned = nbrs
            .iter()
            .filter(|nbr| state.get_partition_of(nbr).is_some())
            .count() as f64;

        self.theta * (num_nbrs_partitioned / degree) + (degree / self.buffer_deg_threshold)
    }
}
