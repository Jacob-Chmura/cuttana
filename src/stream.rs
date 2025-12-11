/// A type alias for a vertex and it's neighbors
type VertexStreamEntry<T> = (T, Vec<T>);

/// A pull-based vertex-stream. Consumers call `next_entry()` until None
pub trait VertexStream {
    type VertexID;

    fn next_entry(&mut self) -> Option<VertexStreamEntry<Self::VertexID>>;
}

pub struct AdjacencyList<T> {
    data: Vec<(T, Vec<T>)>,
    pos: usize,
}

impl<T> AdjacencyList<T> {
    pub fn new(data: Vec<VertexStreamEntry<T>>) -> Self {
        Self { data, pos: 0 }
    }
}

impl<T: Copy> VertexStream for AdjacencyList<T> {
    type VertexID = T;

    fn next_entry(&mut self) -> Option<VertexStreamEntry<Self::VertexID>> {
        let out = self.data.get(self.pos)?.clone();
        self.pos += 1;
        Some(out)
    }
}

impl<T: Copy> Iterator for AdjacencyList<T> {
    type Item = VertexStreamEntry<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry()
    }
}
