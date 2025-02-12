use super::sst::read::SstReader;
use crate::memory::record::Record;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, RwLock,
};

pub struct LsmDisk {
    pub(crate) dir: String,

    // Level 0, from oldest to newest
    pub(crate) level_0_counter: AtomicUsize,
    pub(crate) level_0: Arc<RwLock<Vec<SstReader>>>,

    // Level 1, from oldest to newest
    pub(crate) level_1_counter: AtomicUsize,
    pub(crate) level_1: Arc<RwLock<Vec<SstReader>>>,

    // Level 2, from newest to oldest
    pub(crate) level_2_counter: AtomicUsize,
    pub(crate) level_2: Arc<RwLock<Vec<SstReader>>>,

    // Level 3, from newest to oldest
    pub(crate) level_3_counter: AtomicUsize,
    pub(crate) level_3: Arc<RwLock<Vec<SstReader>>>,
}

// the only API is get.
impl LsmDisk {
    pub fn empty(dir: String) -> Self {
        // create level subdirs if not exists
        std::fs::create_dir_all(format!("{}/level_0", dir)).unwrap();
        std::fs::create_dir_all(format!("{}/level_1", dir)).unwrap();
        std::fs::create_dir_all(format!("{}/level_2", dir)).unwrap();
        std::fs::create_dir_all(format!("{}/level_3", dir)).unwrap();

        Self {
            dir,
            level_0: Arc::new(RwLock::new(Vec::new())),
            level_1: Arc::new(RwLock::new(Vec::new())),
            level_2: Arc::new(RwLock::new(Vec::new())),
            level_3: Arc::new(RwLock::new(Vec::new())),
            level_0_counter: AtomicUsize::new(0),
            level_1_counter: AtomicUsize::new(0),
            level_2_counter: AtomicUsize::new(0),
            level_3_counter: AtomicUsize::new(0),
        }
    }

    pub fn new(dir: String) -> Self {
        todo!("Properly initialize all readers and counters !");

        Self {
            dir,
            level_0: todo!(),
            level_1: todo!(),
            level_2: todo!(),
            level_3: todo!(),
            level_0_counter: todo!(),
            level_1_counter: todo!(),
            level_2_counter: todo!(),
            level_3_counter: todo!(),
        }
    }

    pub(crate) fn get_next_l0_relpath(&self) -> String {
        format!(
            // use 8 digits in rel path
            "level_0/sst_{:08}",
            self.level_0_counter.fetch_add(1, Ordering::Relaxed)
        )
    }

    pub(crate) fn add_l0_sst(&self, replath: &str) {
        assert!(
            replath.starts_with("level_0/"),
            "relpath must start with `level_0/`"
        );

        self.level_0.write().unwrap().push(SstReader {
            dir: self.dir.clone(),
            filename: replath.to_string(),
        });
    }

    pub fn get_next_l1_relpath(&self) -> String {
        format!(
            // use 8 digits in rel path
            "level_1/sst_{:08}",
            self.level_1_counter.fetch_add(1, Ordering::Relaxed)
        )
    }

    pub fn get_next_l2_relpath(&self) -> String {
        format!(
            // use 8 digits in rel path
            "level_2/sst_{:08}",
            self.level_2_counter.fetch_add(1, Ordering::Relaxed)
        )
    }

    pub fn get_next_l3_relpath(&self) -> String {
        format!(
            // use 8 digits in rel path
            "level_3/sst_{:08}",
            self.level_3_counter.fetch_add(1, Ordering::Relaxed)
        )
    }
}

impl LsmDisk {
    pub fn get(&self, key: &[u8]) -> Option<Record> {
        // Use .rev() to search from newest to oldest
        for sst in self.level_0.read().unwrap().iter().rev() {
            let val = sst.get(key);
            if val.is_some() {
                return val;
            }
        }

        // Use .rev() to search from newest to oldest
        for sst in self.level_1.read().unwrap().iter().rev() {
            let val = sst.get(key);
            if val.is_some() {
                return val;
            }
        }

        // Use .rev() to search from newest to oldest
        for sst in self.level_2.read().unwrap().iter().rev() {
            let val = sst.get(key);
            if val.is_some() {
                return val;
            }
        }

        // Use .rev() to search from newest to oldest
        for sst in self.level_3.read().unwrap().iter().rev() {
            let val = sst.get(key);
            if val.is_some() {
                return val;
            }
        }

        None
    }
}
