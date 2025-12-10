use cuttana::cuttana_partition;
use cuttana::stream::AdjacencyList;
use std::collections::HashMap;

#[test]
fn test_cuttana() {
    let data = vec![(0, vec![1, 2]), (1, vec![0]), (2, vec![0])];
    let stream = AdjacencyList::new(data.clone());
    const NUM_PARTITIONS: usize = 16;
    const MAX_PARTITION_SIZE: usize = 1024;
    const MAX_BUFFER_SIZE: usize = 1_000_000;
    const DEGREE_MAX: usize = 1000;

    let _p: HashMap<i32, usize> = cuttana_partition(
        stream,
        NUM_PARTITIONS,
        MAX_PARTITION_SIZE,
        MAX_BUFFER_SIZE,
        DEGREE_MAX,
    );
}
