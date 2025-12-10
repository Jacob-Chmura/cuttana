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
