use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct DiskConfig {
    pub(crate) level_0_size_threshold: usize,
    pub(crate) block_size_multiplier: usize,

    pub(crate) level_0_threshold: usize,
    pub(crate) level_1_threshold: usize,
    pub(crate) level_2_threshold: usize,
}
