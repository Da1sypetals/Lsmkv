use super::{memory::MemoryConfig, sst::SstConfig};
use crate::{
    disk::disk::LsmDisk,
    lsmtree::tree::LsmTree,
    memory::{memory::LsmMemory, memtable::Memtable},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    sync::{atomic::AtomicUsize, Arc, RwLock},
};

#[derive(Serialize, Deserialize)]
pub struct LsmConfig {
    path: String,
    memory: MemoryConfig,
    sst: SstConfig,
}

impl LsmConfig {
    pub fn build_empty(self, dir: String) -> LsmTree {
        let mem = LsmMemory {
            active: Arc::new(Memtable::new()),
            active_size: AtomicUsize::new(0),
            frozen: VecDeque::new(),
        };

        LsmTree {
            mem: Arc::new(RwLock::new(mem)),
            mem_config: self.memory,
            disk: LsmDisk::empty(dir),
        }
    }
}
