use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct MemoryConfig {
    pub(crate) freeze_size: usize,
}
