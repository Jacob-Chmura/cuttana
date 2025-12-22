#[derive(Debug, Clone)]
struct MinNode<T> {
    min: Option<(T, usize)>, // (score, index)
}

impl<T: Ord + Clone> MinNode<T> {
    fn empty() -> Self {
        Self { min: None }
    }

    fn add(&mut self, idx: usize, score: T) {
        assert!(self.min.is_none());
        self.min = Some((score, idx));
    }

    fn update(&mut self, idx: usize, score: T) {
        assert!(self.min.as_ref().is_some_and(|(_, i)| *i == idx));
        self.min = Some((score, idx));
    }

    fn remove(&mut self, idx: usize) {
        assert!(self.min.as_ref().is_some_and(|(_, i)| *i == idx));
        self.min = None;
    }

    fn merge(l: &Self, r: &Self) -> Self {
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
pub(crate) struct SegmentTree<T>
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

    pub fn add(&mut self, idx: usize, val: T) -> usize {
        self._add(0, self.n - 1, 1, idx, val)
    }

    pub fn update(&mut self, idx: usize, val: T, i: usize) {
        assert!(self.buf[idx].min.as_ref().is_some_and(|(_, i)| *i == idx));
        self.buf[i].update(idx, val);
        self.prop(i >> 1);
    }

    pub fn remove(&mut self, idx: usize, i: usize) {
        assert!(self.buf[idx].min.as_ref().is_some_and(|(_, i)| *i == idx));
        self.buf[i].remove(idx);
        self.prop(i >> 1);
    }

    pub fn get_min(&self) -> (T, usize) {
        self.query(0, self.n - 1).min.unwrap()
    }

    fn query(&self, l: usize, r: usize) -> MinNode<T> {
        self._query(0, self.n - 1, 1, l, r)
    }

    fn _query(
        &self,
        l: usize,
        r: usize,
        cur_ind: usize,
        target_l: usize,
        target_r: usize,
    ) -> MinNode<T> {
        if target_l > target_r {
            return MinNode::empty();
        }

        if l == target_l && r == target_r {
            return self.buf[cur_ind].clone();
        }

        let mid: usize = l + (r - l) / 2;
        let node_l = self._query(l, mid, 2 * cur_ind, target_l, mid.min(target_r));
        let node_r = self._query(
            mid + 1,
            r,
            2 * cur_ind + 1,
            (mid + 1).max(target_l),
            target_r,
        );
        MinNode::merge(&node_l, &node_r)
    }

    fn build(&mut self, l: usize, r: usize, i: usize) {
        if l == r {
            self.buf[i] = MinNode::empty();
            return;
        }

        let mid: usize = l + (r - l) / 2;
        self.build(l, mid, 2 * i);
        self.build(mid + 1, r, 2 * i + 1);
        self.buf[i] = MinNode::merge(&self.buf[2 * i], &self.buf[2 * i + 1]);
    }

    fn prop(&mut self, i: usize) {
        if i > 0 {
            self.buf[i] = MinNode::merge(&self.buf[2 * i], &self.buf[2 * i + 1]);
            self.prop(i >> 1);
        }
    }

    fn _add(&mut self, l: usize, r: usize, i: usize, idx: usize, score: T) -> usize {
        if l == r {
            assert!(self.buf[i].min.is_none());
            self.buf[i].add(idx, score);
            return i;
        }

        let mid: usize = l + (r - l) / 2;
        let pos: Option<usize>;
        if self.buf[2 * i].min.is_none() {
            pos = Some(self._add(l, mid, 2 * i, idx, score));
        } else {
            pos = Some(self._add(mid + 1, r, 2 * i + 1, idx, score));
        }

        self.buf[i] = MinNode::merge(&self.buf[2 * i], &self.buf[2 * i + 1]);
        assert!(pos.is_some());
        pos.unwrap()
    }
}
