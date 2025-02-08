use std::{
    collections::VecDeque,
    sync::{atomic::AtomicUsize, Arc, RwLock},
};

use crate::{
    lsmtree::tree::LsmTree,
    memory::{config::LsmMemoryConfig, memory::LsmMemory, memtable::Memtable},
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct LsmConfig {
    path: String,
    memory: LsmMemoryConfig,
}

impl LsmConfig {
    pub fn empty(self) -> LsmTree {
        let mem = LsmMemory {
            active: Arc::new(Memtable::new()),
            active_size: AtomicUsize::new(0),
            frozen: VecDeque::new(),
        };

        LsmTree {
            mem: Arc::new(RwLock::new(mem)),
            mem_config: self.memory,
        }
    }
}
