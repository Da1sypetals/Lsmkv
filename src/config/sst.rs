use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct SstConfig {
    pub(crate) block_size: u16,

    // Bloom filter
    /// Expected data scale
    pub(crate) scale: usize,
    /// Expected false positive rate
    pub(crate) fpr: f64,
}
