use cuttana::partition;
use cuttana::result::PartitionResult;
use cuttana::stream::VertexStream;

#[test]
fn test_cuttana() {
    let data = vec![(0, vec![1, 2]), (1, vec![0]), (2, vec![0])];
    let stream = VertexStream::from_adjacency_list(data);
    const NUM_PARTITIONS: u8 = 16;
    const MAX_PARTITION_SIZE: u32 = 1024;
    const MAX_BUFFER_SIZE: u64 = 1_000_000;
    const DEGREE_MAX: u32 = 1000;

    let result: PartitionResult<i32> = partition(
        stream,
        NUM_PARTITIONS,
        MAX_PARTITION_SIZE,
        MAX_BUFFER_SIZE,
        DEGREE_MAX,
    );

    assert_eq!(result.num_partitions, NUM_PARTITIONS);
    assert_eq!(result.vertex_count, 3);
    assert_eq!(result.edge_count, 4);
}
