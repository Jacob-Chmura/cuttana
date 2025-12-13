#[derive(Clone, Debug)]
pub struct CuttanaConfig {
    pub num_sub_partitions: u16,
    pub max_sub_partition_size: u32,
    pub max_buffer_size: u64,
    pub buffer_degree_threshold: u32,
    pub gamma: f64,
    pub theta: f64,
}

impl Default for CuttanaConfig {
    fn default() -> Self {
        // TODO: organize
        Self {
            num_sub_partitions: 4096,
            max_sub_partition_size: 1_000_000,
            max_buffer_size: 1_000_000,
            buffer_degree_threshold: 100,
            gamma: 1.5,
            theta: 2.0,
        }
    }
}
