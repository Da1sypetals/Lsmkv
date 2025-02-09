use super::sst::read::SstReader;

pub struct LsmDisk {
    dir: String,

    level_0: Vec<SstReader>,
    level_1: Vec<SstReader>,
    level_2: Vec<SstReader>,
    level_3: Vec<SstReader>,
}

// the only API is get.
impl LsmDisk {
    pub fn new(&self, dir: String) -> Self {
        todo!("Initialize levels");
        Self {
            dir,
            level_0: todo!(),
            level_1: todo!(),
            level_2: todo!(),
            level_3: todo!(),
        }
    }

    pub fn get(&self, key: &[u8]) {
        //
    }
}
