use cuttana::config::CuttanaConfig;
use cuttana::partition;
use cuttana::stream::{Delimiter, VertexStream};

fn main() -> std::io::Result<()> {
    let path = "./examples/emailEnron.txt";
    let stream = VertexStream::<i32>::from_csv(path, Delimiter::Space)?;

    let num_partitions = 16;
    let config = CuttanaConfig::default();
    let result = partition(stream, num_partitions, config);

    println!("Vertices: {}", result.vertex_count);
    println!("Edges: {}", result.edge_count);
    println!("Edge cut ratio: {:.3}", result.edge_cut_ratio);
    println!("Communication volume: {:.3}", result.communication_volume);

    Ok(())
}
