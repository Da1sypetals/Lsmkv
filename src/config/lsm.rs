use super::{disk::DiskConfig, memory::MemoryConfig, sst::SstConfig};
use crate::{
    disk::disk::LsmDisk,
    lsmtree::{signal::Signal, tree::LsmTree},
    memory::{memory::LsmMemory, memtable::Memtable},
};
use scc::Queue;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc, RwLock,
    },
};

#[derive(Serialize, Deserialize, Clone)]
pub struct LsmConfig {
    pub(crate) dir: String,
    pub(crate) memory: MemoryConfig,
    pub(crate) sst: SstConfig,
    pub(crate) disk: DiskConfig,
}

impl LsmConfig {
    pub fn build_empty(self, dir: String) -> LsmTree {
        let mem = LsmMemory {
            active: Arc::new(Memtable::new()),
            active_size: AtomicUsize::new(0),
            frozen: Queue::default(),
            frozen_sizes: Queue::default(),
        };

        LsmTree {
            mem: Arc::new(RwLock::new(mem)),
            config: self.clone(),
            disk: LsmDisk::empty(self, false),
            // currently not used
            flush_signal: Arc::new(Signal::new()),
            // currently not used
            flush_handle: None,
        }
    }
}
