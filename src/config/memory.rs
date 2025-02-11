use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct MemoryConfig {
    pub(crate) freeze_size: usize,
    pub(crate) flush_size: usize,
}
