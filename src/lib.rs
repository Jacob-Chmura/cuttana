use std::collections::HashMap;

/// Tracks vertex-based partion assignment output from a graph partioner
pub struct PartitionState<T> {
    assignments: HashMap<T, usize>,
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

pub fn cuttana_partition<T>(
    stream: impl VertexStream,
    num_partitions: usize,
    num_sub_partitions: usize,
    balance_slack: f64,
    gamma: f64,
    info_gain_threshold: f64,
    buffer_max_size: usize,
) -> PartitionState<T> {
    unimplemented!();
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
}
