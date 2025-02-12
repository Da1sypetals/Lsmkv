use super::{memory::MemoryConfig, sst::SstConfig};
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
    pub(crate) path: String,
    pub(crate) memory: MemoryConfig,
    pub(crate) sst: SstConfig,
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
            config: self,
            disk: Arc::new(LsmDisk::empty(dir)),
            // currently not used
            flush_signal: Arc::new(Signal::new()),
            // currently not used
            flush_handle: None,
            terminated: Arc::new(AtomicBool::new(false)),
        }
    }
}
