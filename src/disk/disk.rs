use crate::memory::record::Record;

use super::sst::read::SstReader;

pub struct LsmDisk {
    pub(crate) dir: String,

    pub(crate) level_0: Vec<SstReader>,
    pub(crate) level_1: Vec<SstReader>,
    pub(crate) level_2: Vec<SstReader>,
    pub(crate) level_3: Vec<SstReader>,
}

// the only API is get.
impl LsmDisk {
    pub fn empty(dir: String) -> Self {
        Self {
            dir,
            level_0: Vec::new(),
            level_1: Vec::new(),
            level_2: Vec::new(),
            level_3: Vec::new(),
        }
    }

    pub fn new(dir: String) -> Self {
        todo!("Initialize levels");
        Self {
            dir,
            level_0: todo!(),
            level_1: todo!(),
            level_2: todo!(),
            level_3: todo!(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Record> {
        for sst in &self.level_0 {
            let val = sst.get(key);
            if val.is_some() {
                return val;
            }
        }

        for sst in &self.level_1 {
            let val = sst.get(key);
            if val.is_some() {
                return val;
            }
        }

        for sst in &self.level_2 {
            let val = sst.get(key);
            if val.is_some() {
                return val;
            }
        }

        for sst in &self.level_3 {
            let val = sst.get(key);
            if val.is_some() {
                return val;
            }
        }

        None
    }
}
