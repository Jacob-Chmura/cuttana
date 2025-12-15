use cuttana::config::CuttanaConfig;
use cuttana::partition;
use cuttana::result::PartitionResult;
use cuttana::stream::VertexStream;

#[test]
fn test_cuttana() {
    let data = vec![(0, vec![1, 2]), (1, vec![0]), (2, vec![0])];
    let stream = VertexStream::from_adjacency_list(data);
    const NUM_PARTITIONS: u8 = 16;
    let config = CuttanaConfig::default();

    let result: PartitionResult<i32> = partition(stream, NUM_PARTITIONS, config);

    assert_eq!(result.vertex_count, 3);
    assert_eq!(result.edge_count, 4);
}
