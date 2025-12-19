#[derive(Clone, Debug)]
pub struct CuttanaConfig {
    pub num_sub_partitions: usize,
    pub balance_slack: f64,
    pub max_buffer_size: usize,
    pub buffer_degree_threshold: u32,
    pub gamma: f64,
    pub sub_gamma: f64,
    pub theta: f64,
    pub info_gain_threshold: u64,
}

impl CuttanaConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        num_sub_partitions: usize,
        balance_slack: f64,
        max_buffer_size: usize,
        buffer_degree_threshold: u32,
        gamma: f64,
        sub_gamma: f64,
        theta: f64,
        info_gain_threshold: u64,
    ) -> Result<Self, &'static str> {
        if balance_slack < 0.0 {
            return Err("balance_slack must be >= 0");
        }

        Ok(Self {
            num_sub_partitions,
            balance_slack,
            max_buffer_size,
            buffer_degree_threshold,
            gamma,
            sub_gamma,
            theta,
            info_gain_threshold,
        })
    }
}

impl Default for CuttanaConfig {
    fn default() -> Self {
        // TODO: organize
        Self::new(
            4096,      // num_sub_partitions
            0.05,      // balance_slack
            1_000_000, // max_buffer_size
            100,       // buffer_degree_threshold
            1.5,       // gamma
            1.0,       // sub_gamma
            2.0,       // theta
            0,         // info_gain_threshold
        )
        .unwrap()
    }
}
