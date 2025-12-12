use cuttana::cuttana_partition;
use cuttana::result::PartitionResult;
use cuttana::stream::AdjacencyList;

#[test]
fn test_cuttana() {
    let data = vec![(0, vec![1, 2]), (1, vec![0]), (2, vec![0])];
    let stream = AdjacencyList::new(data.clone());
    const NUM_PARTITIONS: usize = 16;
    const MAX_PARTITION_SIZE: usize = 1024;
    const MAX_BUFFER_SIZE: usize = 1_000_000;
    const DEGREE_MAX: usize = 1000;

    let result: PartitionResult<i32> = cuttana_partition(
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
