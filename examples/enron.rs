use cuttana::partition;
use cuttana::stream::{Delimiter, VertexStream};

fn main() -> std::io::Result<()> {
    let path = "./examples/emailEnron.txt";
    let stream = VertexStream::<i32>::from_csv(path, Delimiter::Space)?;

    let num_partitions = 16;
    let max_partition_size = 10000;
    let max_buffer_size = 100;
    let buffer_degree_threshold = 10;

    let result = partition(
        stream,
        num_partitions,
        max_partition_size,
        max_buffer_size,
        buffer_degree_threshold,
    );

    println!("Vertices: {}", result.vertex_count);
    println!("Edges: {}", result.edge_count);
    println!("Edge cut ratio: {:.3}", result.edge_cut_cost);
    println!(
        "Communication volume: {:.3}",
        result.communication_volume_cost
    );

    Ok(())
}
