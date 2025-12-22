#[derive(Debug, Clone)]
struct MinNode<T> {
    min: Option<(T, usize)>, // (score, elem)
}

impl<T: Ord + Clone> MinNode<T> {
    fn empty() -> Self {
        Self { min: None }
    }

    fn add(&mut self, elem: usize, score: T) {
        debug_assert!(self.min.is_none());
        self.min = Some((score, elem));
    }

    fn update(&mut self, elem: usize, score: T) {
        debug_assert!(self.min.as_ref().is_some_and(|(_, i)| *i == elem));
        self.min = Some((score, elem));
    }

    fn remove(&mut self, elem: usize) {
        debug_assert!(self.min.as_ref().is_some_and(|(_, i)| *i == elem));
        self.min = None;
    }

    fn merged(l: &Self, r: &Self) -> Self {
        use std::cmp::Ordering::*;

        let min = match (&l.min, &r.min) {
            (None, None) => None,
            (Some(_), None) => l.min.clone(),
            (None, Some(_)) => r.min.clone(),
            (Some((ls, li)), Some((rs, ri))) => match ls.cmp(rs) {
                Less => l.min.clone(),
                Greater => r.min.clone(),
                Equal => Some((ls.clone(), (*li).min(*ri))),
            },
        };

        Self { min }
    }
}

#[derive(Debug, Clone)]
pub struct SegmentTree<T>
where
    T: Ord + Clone,
{
    buf: Vec<MinNode<T>>,
    n: usize,
}

impl<T> SegmentTree<T>
where
    T: Ord + Clone,
{
    pub fn new(n: usize) -> Self {
        let mut tree = Self {
            buf: vec![MinNode::empty(); 4 * n],
            n,
        };
        tree.build(0, n - 1, 1);
        tree
    }

    /// Adds an element with a score.
    /// Returns a handle (tree index) used for update/remove.
    pub fn add(&mut self, elem: usize, score: T) -> usize {
        self.add_rec(0, self.n - 1, 1, elem, score)
    }

    pub fn update(&mut self, elem: usize, score: T, node: usize) {
        debug_assert!(self.buf[node].min.as_ref().is_some_and(|(_, i)| *i == elem));
        self.buf[node].update(elem, score);
        self.prop(node >> 1);
    }

    pub fn remove(&mut self, elem: usize, node: usize) {
        debug_assert!(self.buf[node].min.as_ref().is_some_and(|(_, i)| *i == elem));
        self.buf[node].remove(elem);
        self.prop(node >> 1);
    }

    pub fn get_min(&self) -> (T, usize) {
        self.buf[1].min.clone().unwrap()
    }

    fn build(&mut self, l: usize, r: usize, i: usize) {
        if l == r {
            self.buf[i] = MinNode::empty();
            return;
        }

        let mid = l + (r - l) / 2;
        self.build(l, mid, 2 * i);
        self.build(mid + 1, r, 2 * i + 1);
        self.buf[i] = MinNode::merged(&self.buf[2 * i], &self.buf[2 * i + 1]);
    }

    fn prop(&mut self, mut i: usize) {
        while i > 0 {
            self.buf[i] = MinNode::merged(&self.buf[2 * i], &self.buf[2 * i + 1]);
            i >>= 1;
        }
    }

    fn add_rec(&mut self, l: usize, r: usize, i: usize, elem: usize, score: T) -> usize {
        if l == r {
            debug_assert!(self.buf[i].min.is_none());
            self.buf[i].add(elem, score);
            return i;
        }

        let mid = l + (r - l) / 2;
        let pos = if self.buf[2 * i].min.is_none() {
            self.add_rec(l, mid, 2 * i, elem, score)
        } else {
            self.add_rec(mid + 1, r, 2 * i + 1, elem, score)
        };

        self.buf[i] = MinNode::merged(&self.buf[2 * i], &self.buf[2 * i + 1]);
        pos
    }
}
