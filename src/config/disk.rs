use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct DiskConfig {
    /// L1 block size = L0 block size * block_size_multiplier, etc.
    pub(crate) block_size_multiplier: usize,

    pub(crate) level_0_threshold: usize,
    pub(crate) level_1_threshold: usize,
    pub(crate) level_2_threshold: usize,
}
