use cuttana::stream::{AdjacencyList, VertexStream};

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
