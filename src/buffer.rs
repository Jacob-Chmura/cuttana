use crate::state::CuttanaState;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::hash::Hash;

/// Manages buffered vertices which are not ready to partition
pub(crate) struct BufferManager<T, S>
where
    T: Eq + Clone + Hash + Ord,
    S: BufferScorer,
{
    tree: BTreeMap<BufferKey<T>, Vec<T>>, // key: (score, vertex) -> nbrs)
    map: HashMap<T, f64>,                 // vertex -> score
    capacity: u64,
    scorer: S,
}

impl<T, S> BufferManager<T, S>
where
    T: Eq + Clone + Hash + Ord,
    S: BufferScorer,
{
    pub fn new(capacity: u64, scorer: S) -> Self {
        Self {
            tree: BTreeMap::new(),
            map: HashMap::new(),
            capacity,
            scorer,
        }
    }

    pub fn is_at_capacity(&self) -> bool {
        self.map.len() as u64 >= self.capacity
    }

    pub fn insert(&mut self, v: &T, nbrs: &[T], state: &CuttanaState<T>) {
        let score = self.scorer.score(v, nbrs, state);
        let key = BufferKey {
            score,
            vertex: v.clone(),
        };
        self.map.insert(v.clone(), score);
        self.tree.insert(key, nbrs.to_vec());
    }

    pub fn evict(&mut self) -> Option<(T, Vec<T>)> {
        if let Some((key, nbrs)) = self.tree.last_key_value() {
            let v = key.vertex.clone();
            let score = key.score;
            let nbrs_cloned = nbrs.clone();

            self.tree.remove(&BufferKey {
                score,
                vertex: v.clone(),
            });
            self.map.remove(&v);

            return Some((v, nbrs_cloned));
        }
        None
    }

    pub fn update_score(&mut self, v: &T, state: &CuttanaState<T>) {
        let old_score = match self.map.get(v).copied() {
            Some(s) => s,
            None => return,
        };

        let key = BufferKey {
            score: old_score,
            vertex: v.clone(),
        };
        let nbrs = self.tree.remove(&key).unwrap();

        let new_score = self.scorer.update_score(old_score, v, &nbrs, state);
        let new_key = BufferKey {
            score: new_score,
            vertex: v.clone(),
        };

        self.tree.insert(new_key, nbrs);
        self.map.insert(v.clone(), new_score);
    }
}

#[derive(Clone)]
pub(crate) struct BufferKey<T> {
    pub score: f64,
    pub vertex: T,
}

impl<T: Ord> PartialEq for BufferKey<T> {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.vertex == other.vertex
    }
}
impl<T: Ord> Eq for BufferKey<T> {}

impl<T: Ord> PartialOrd for BufferKey<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for BufferKey<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.score.partial_cmp(&other.score) {
            Some(std::cmp::Ordering::Equal) => self.vertex.cmp(&other.vertex),
            Some(ord) => ord,
            None => std::cmp::Ordering::Equal,
        }
    }
}

pub(crate) trait BufferScorer {
    fn score<T: Eq + Hash + Clone>(&self, v: &T, nbrs: &[T], state: &CuttanaState<T>) -> f64;

    fn update_score<T: Eq + Hash + Clone>(
        &self,
        _old_score: f64,
        v: &T,
        nbrs: &[T],
        state: &CuttanaState<T>,
    ) -> f64 {
        self.score(v, nbrs, state)
    }
}

pub(crate) struct CuttanaBufferScorer {
    theta: f64,
    buffer_deg_threshold: f64,
}

impl CuttanaBufferScorer {
    pub fn new(theta: f64, buffer_deg_threshold: u32) -> Self {
        Self {
            theta,
            buffer_deg_threshold: buffer_deg_threshold as f64,
        }
    }
}

impl BufferScorer for CuttanaBufferScorer {
    fn score<T: Eq + Hash + Clone>(&self, _v: &T, nbrs: &[T], state: &CuttanaState<T>) -> f64 {
        let degree = nbrs.len() as f64;
        let num_nbrs_partitioned = nbrs
            .iter()
            .filter(|nbr| state.global.partition_of(nbr).is_some())
            .count() as f64;

        self.theta * (num_nbrs_partitioned / degree) + (degree / self.buffer_deg_threshold)
    }

    fn update_score<T: Eq + Hash + Clone>(
        &self,
        old_score: f64,
        _v: &T,
        nbrs: &[T],
        _state: &CuttanaState<T>,
    ) -> f64 {
        old_score + self.theta / nbrs.len() as f64
    }
}
