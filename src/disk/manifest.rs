use crate::disk::disk::Level;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct LevelManifest {
    pub(crate) counter: usize,
    pub(crate) filenames: Vec<String>,
}

impl LevelManifest {
    pub(crate) fn from_level(level: &Level) -> Self {
        let guard = level.read().unwrap();
        Self {
            counter: guard.counter,
            filenames: guard
                .sst_readers
                .iter()
                .map(|r| r.filename.clone())
                .collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Manifest {
    pub(crate) level_0: LevelManifest,
    pub(crate) level_1: LevelManifest,
    pub(crate) level_2: LevelManifest,
    pub(crate) level_3: LevelManifest,
}
