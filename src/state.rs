use crate::assignment::PartitionAssignment;
use crate::config::CuttanaConfig;
use std::collections::HashMap;
use std::hash::Hash;

pub(crate) struct PartitionInfo {
    pub num_sub: u16,
    pub _move_score: Vec<i32>, // TODO: Segment Trees
}

impl PartitionInfo {
    pub fn new(num_partitions: u8, num_sub: u16) -> Self {
        Self {
            num_sub,
            _move_score: vec![0; num_partitions as usize],
        }
    }
}

pub(crate) struct SubPartitionInfo {
    pub parent: u8,
    pub edges: HashMap<u16, u64>,
    pub edge_cuts: Vec<u64>,
}

impl SubPartitionInfo {
    pub fn new(parent: u8, num_partitions: u8) -> Self {
        Self {
            parent,
            edges: HashMap::new(),
            edge_cuts: vec![0; num_partitions as usize],
        }
    }

    pub fn add_edge(&mut self, other: u16) {
        *self.edges.entry(other).or_insert(0) += 1;
    }

    pub fn get_edge(&self, other: u16) -> u64 {
        self.edges.get(&other).copied().unwrap_or(0)
    }
}

/// Cuttana Partioning State
pub(crate) struct CuttanaState<T> {
    pub global_assignments: PartitionAssignment<T, u8>,
    pub local_assignments: HashMap<u8, PartitionAssignment<T, u16>>,
    pub partitions: Vec<PartitionInfo>,
    pub sub_partitions: Vec<SubPartitionInfo>,
}

impl<T> CuttanaState<T>
where
    T: Eq + Hash,
{
    pub fn new(num_partitions: u8, config: &CuttanaConfig) -> Self {
        // Adding extra slack for `Phase 1` of the algorithm. E.g. see:
        // https://github.com/cuttana/cuttana-partitioner/blob/ed0c18251273a41792c1fc3e909d4ced44beaa27/partitioners/ogpart_single_thread.cpp#L167
        let balance_slack = (config.balance_slack * 2.0).min(config.balance_slack + 0.5);
        let num_sub_partitions = config.num_sub_partitions;
        let total_sub_partitions = num_sub_partitions as u64 * num_partitions as u64;

        Self {
            global_assignments: PartitionAssignment::new(num_partitions, balance_slack),
            local_assignments: (0..num_partitions)
                .map(|g| {
                    (
                        g,
                        PartitionAssignment::new(num_sub_partitions, balance_slack),
                    )
                })
                .collect(),
            partitions: (0..num_partitions)
                .map(|_| PartitionInfo::new(num_partitions, num_sub_partitions))
                .collect(),
            sub_partitions: (0..total_sub_partitions)
                .map(|id| {
                    let parent = id / num_sub_partitions as u64;
                    SubPartitionInfo::new(parent as u8, num_partitions)
                })
                .collect(),
        }
    }

    #[inline]
    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    #[inline]
    pub fn num_sub_partitions(&self) -> usize {
        self.sub_partitions.len()
    }

    pub fn local_assignment_for(&mut self, partition: u8) -> &mut PartitionAssignment<T, u16> {
        self.local_assignments
            .get_mut(&partition)
            .expect("Global partition does not exist")
    }

    pub fn update_metrics(&mut self, _v: &T, nbrs: &[T]) {
        let num_partitions = self.num_partitions() as u64;
        let metrics = &mut self.global_assignments.metrics;
        metrics.vertex_count += 1;
        metrics.edge_count += nbrs.len() as u64;

        let v_eff = metrics.vertex_count / num_partitions;
        let e_eff = metrics.edge_count / num_partitions;
        for p in 0..num_partitions {
            self.local_assignment_for(p as u8).metrics.vertex_count = v_eff;
            self.local_assignment_for(p as u8).metrics.edge_count = e_eff;
        }
    }

    pub fn compute_sub_partition_edge_cuts(&mut self) {
        let parents: Vec<usize> = self
            .sub_partitions
            .iter()
            .map(|s| s.parent as usize)
            .collect();

        for i in 0..self.sub_partitions.len() {
            let sub = &mut self.sub_partitions[i];
            let edge_cuts = &mut sub.edge_cuts;

            edge_cuts.fill(0);
            let mut total_cut: u64 = 0;

            // subtract edge weights for the partition of each adjacent sub-partition
            for (&nbr, &weight) in &sub.edges {
                total_cut += weight;
                edge_cuts[parents[nbr as usize]] -= weight;
            }

            // add total edge cut to all partitions
            edge_cuts.iter_mut().for_each(|x| *x += total_cut);
        }
    }
}
