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
