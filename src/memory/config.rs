use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct LsmMemoryConfig {
    pub(crate) freeze_size: usize,
}
