use std::sync::RwLock;

use crate::disk::disk::Level;
use serde::{Deserialize, Serialize};

use super::{disk::LevelInner, sst::read::SstReader};

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

    pub(crate) fn from_level_guard(level: &Level) -> Self {
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

    pub(crate) fn as_level(&self, dir: &str, level: usize) -> Level {
        let inner = LevelInner {
            level,
            counter: self.counter,
            sst_readers: self
                .filenames
                .iter()
                .map(|filename| SstReader {
                    dir: dir.to_string(),
                    filename: filename.to_string(),
                })
                .collect(),
        };

        RwLock::new(inner)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Manifest {
    pub(crate) level_0: LevelManifest,
    pub(crate) level_1: LevelManifest,
    pub(crate) level_2: LevelManifest,
    pub(crate) level_3: LevelManifest,
}
