use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct SstConfig {
    pub(crate) block_size: u16,
    
}
