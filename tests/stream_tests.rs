use cuttana::stream::VertexStream;

#[test]
fn test_iterate_adj_list() {
    let data = vec![(0, vec![1, 2]), (1, vec![0]), (2, vec![0])];
    let mut stream = VertexStream::from_adjacency_list(data.clone());

    let mut seen = vec![];
    for (v, nbrs) in &mut stream {
        seen.push((v, nbrs));
    }
    assert_eq!(seen, data);
}

#[test]
fn test_empty_adj_list() {
    let mut stream = VertexStream::<usize>::from_adjacency_list(vec![]);
    assert!(stream.next().is_none());
}
