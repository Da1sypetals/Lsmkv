use super::log_record::LogRecord;
use super::replay::WalReplayer;
use crate::memory::memory::LsmMemory;
use crate::memory::memtable::Memtable;
use scc::Queue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::{
    collections::VecDeque,
    fs::File,
    io::{BufWriter, Write},
};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub struct WalManifest {
    pub(crate) active: String,
    pub(crate) frozen: Vec<String>,
}

pub enum WalValue<'a> {
    Value(&'a [u8]),
    Tomb,
}

pub struct Wal {
    pub(crate) dir: String,
    pub(crate) active_path: String,
    pub(crate) active: BufWriter<File>,

    /// Paths to wal files
    pub(crate) frozen: VecDeque<String>,
}

impl Wal {
    pub fn empty(dir: String) -> Self {
        let frozen = VecDeque::new();
        let dir = dir.clone();

        // create dir for wal
        std::fs::create_dir_all(format!("{}/wal", dir)).unwrap();

        let active_path = format!("{}/wal/wal-{}.log", dir, Uuid::new_v4().to_string());
        let active = BufWriter::new(File::create(&active_path).unwrap());

        let mut wal = Wal {
            dir,
            active_path,
            active,
            frozen,
        };

        wal.update_manifest();

        wal
    }

    pub fn log(&mut self, record: LogRecord) {
        match record {
            LogRecord::Value {
                key,
                value,
                timestamp,
            } => {
                self.active.write_all(&[0]).unwrap();

                // encode -----------------------------------------------------
                // write timestamp
                let timestamp_bytes = timestamp.to_le_bytes();
                self.active.write_all(&timestamp_bytes).unwrap();

                // write key size
                let key_size = (key.len() as u16).to_le_bytes();
                self.active.write_all(&key_size).unwrap();

                // write value size
                let value_size = (value.len() as u16).to_le_bytes();
                self.active.write_all(&value_size).unwrap();

                // write key
                self.active.write_all(key).unwrap();

                // write value
                self.active.write_all(value).unwrap();
                // encode -----------------------------------------------------
            }
            LogRecord::Tomb { key, timestamp } => {
                // encode -----------------------------------------------------
                // write tombstone
                self.active.write_all(&[1]).unwrap();

                // write timestamp
                let timestamp_bytes = timestamp.to_le_bytes();
                self.active.write_all(&timestamp_bytes).unwrap();

                // write key size
                let key_size = (key.len() as u16).to_le_bytes();
                self.active.write_all(&key_size).unwrap();
                // encode -----------------------------------------------------

                // write key
                self.active.write_all(key).unwrap();
            }
            LogRecord::TransactionStart {
                start_timestamp,
                transaction_id,
            } => {
                // encode -----------------------------------------------------
                // write tombstone
                self.active.write_all(&[2]).unwrap();

                // write timestamp
                let timestamp_bytes = start_timestamp.to_le_bytes();
                self.active.write_all(&timestamp_bytes).unwrap();

                // write transaction id
                let transaction_id_bytes = transaction_id.to_le_bytes();
                self.active.write_all(&transaction_id_bytes).unwrap();
                // encode -----------------------------------------------------
            }
            LogRecord::TransactionValue {
                transaction_id,
                key,
                value,
            } => {
                // encode -----------------------------------------------------
                // write transaction value
                self.active.write_all(&[3]).unwrap();

                // write transaction id
                let transaction_id_bytes = transaction_id.to_le_bytes();
                self.active.write_all(&transaction_id_bytes).unwrap();

                // write key size
                let key_size = (key.len() as u16).to_le_bytes();
                self.active.write_all(&key_size).unwrap();

                // write value size
                let value_size = (value.len() as u16).to_le_bytes();
                self.active.write_all(&value_size).unwrap();

                // write key
                self.active.write_all(key).unwrap();

                // write value
                self.active.write_all(value).unwrap();
                // encode -----------------------------------------------------
            }
            LogRecord::TransactionTomb {
                transaction_id,
                key,
            } => {
                // encode -----------------------------------------------------
                // write transaction tombstone
                self.active.write_all(&[4]).unwrap();

                // write transaction id
                let transaction_id_bytes = transaction_id.to_le_bytes();
                self.active.write_all(&transaction_id_bytes).unwrap();

                // write key size
                let key_size = (key.len() as u16).to_le_bytes();
                self.active.write_all(&key_size).unwrap();

                // write key
                self.active.write_all(key).unwrap();
                // encode -----------------------------------------------------
            }
            LogRecord::TransactionEnd {
                end_timestamp,
                transaction_id,
            } => {
                // encode -----------------------------------------------------
                // write transaction end
                self.active.write_all(&[5]).unwrap();

                // write start timestamp
                let start_timestamp_bytes = end_timestamp.to_le_bytes();
                self.active.write_all(&start_timestamp_bytes).unwrap();

                // write transaction id
                let transaction_id_bytes = transaction_id.to_le_bytes();
                self.active.write_all(&transaction_id_bytes).unwrap();
                // encode -----------------------------------------------------
            }
        }

        // And finally, flush
        self.active.flush().unwrap();
    }

    // pub fn log(&mut self, key: &[u8], value: WalValue, timestamp: u64) {
    //     match value {
    //         WalValue::Value(value) => {
    //             self.active.write_all(&[0]).unwrap();

    //             // encode -----------------------------------------------------
    //             // write timestamp
    //             let timestamp_bytes = timestamp.to_le_bytes();
    //             self.active.write_all(&timestamp_bytes).unwrap();

    //             // write key size
    //             let key_size = (key.len() as u16).to_le_bytes();
    //             self.active.write_all(&key_size).unwrap();

    //             // write value size
    //             let value_size = (value.len() as u16).to_le_bytes();
    //             self.active.write_all(&value_size).unwrap();

    //             // write key
    //             self.active.write_all(key).unwrap();

    //             // write value
    //             self.active.write_all(value).unwrap();
    //             // encode -----------------------------------------------------

    //             self.active.flush().unwrap();
    //         }
    //         WalValue::Tomb => {
    //             // encode -----------------------------------------------------
    //             // write tombstone
    //             self.active.write_all(&[1]).unwrap();

    //             // write timestamp
    //             let timestamp_bytes = timestamp.to_le_bytes();
    //             self.active.write_all(&timestamp_bytes).unwrap();

    //             // write key size
    //             let key_size = (key.len() as u16).to_le_bytes();
    //             self.active.write_all(&key_size).unwrap();
    //             // encode -----------------------------------------------------

    //             // write key
    //             self.active.write_all(key).unwrap();

    //             self.active.flush().unwrap();
    //         }
    //     }
    // }

    pub fn freeze_current(&mut self) {
        self.frozen.push_back(self.active_path.clone());

        self.active_path = self.new_filename();
        self.active = BufWriter::new(File::create(&self.active_path).unwrap());

        self.update_manifest();
    }

    pub fn pop_oldest(&mut self) {
        match self.frozen.pop_front() {
            Some(path) => {
                // delete file
                std::fs::remove_file(path).unwrap();
            }
            None => panic!("Should have at least 1 Wal file to pop!"),
        }

        self.update_manifest();
    }
}

impl Wal {
    pub fn load(dir: &str) -> Self {
        let manifest_path = format!("{}/wal/manifest.toml", dir);

        // load manifest
        let manifest_str = std::fs::read_to_string(&manifest_path).unwrap();
        let manifest: WalManifest = toml::from_str(&manifest_str).unwrap();

        // open active file, point cursor to the end
        let active = BufWriter::new(
            std::fs::OpenOptions::new()
                .append(true)
                .open(&manifest.active)
                .unwrap(),
        );

        // Convert frozen paths to VecDeque
        let frozen = VecDeque::from(manifest.frozen);

        Wal {
            dir: dir.to_string(),
            active_path: manifest.active,
            active,
            frozen,
        }
    }

    pub fn replay(&self) -> LsmMemory {
        // Create frozen memtables and their sizes
        let frozen_tables = Queue::default();
        let frozen_sizes = Queue::default();

        // transaction_id -> transaction records mapping
        let mut replayer = WalReplayer::empty();

        // First replay frozen files in order (older to newer)
        for path in &self.frozen {
            let memtable = Arc::new(Memtable::new());
            let mut file = std::fs::File::open(path).unwrap();
            let size = replayer.replay_file(&mut file, &memtable);

            frozen_tables.push(memtable);
            frozen_sizes.push(size);
        }

        // Create and replay active memtable
        let active = Arc::new(Memtable::new());
        let file = std::fs::File::open(&self.active_path).unwrap();
        let active_size = replayer.replay_file(&mut std::io::BufReader::new(file), &active);

        LsmMemory {
            active,
            active_size: AtomicUsize::new(active_size),
            frozen: frozen_tables,
            frozen_sizes,
        }
    }
}

impl Wal {
    fn update_manifest(&mut self) {
        let path = format!("{}/wal/manifest.toml", self.dir);
        let manifest = toml::to_string(&self.current_manifest()).unwrap();

        File::create(path)
            .unwrap()
            .write_all(manifest.as_bytes())
            .unwrap();
    }

    fn current_manifest(&self) -> WalManifest {
        WalManifest {
            active: self.active_path.clone(),
            frozen: self.frozen.iter().cloned().collect(),
        }
    }

    fn new_filename(&self) -> String {
        format!("{}/wal/wal-{}.log", self.dir, Uuid::new_v4().to_string())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::memory::types::Record;
//     use bytes::Bytes;
//     use std::fs;
//     use tempfile::tempdir;

//     #[test]
//     fn test_wal_recovery() {
//         // Create test directory using tempdir
//         let temp = tempdir().unwrap();
//         let test_dir = temp.path().to_str().unwrap();
//         fs::create_dir_all(format!("{}/wal", test_dir)).unwrap();

//         // Create WAL and write data
//         let mut wal = Wal::empty(test_dir.to_string());
//         let mut expected_data = Vec::new();

//         // Write first batch (will be in frozen1)
//         for i in 0..1000 {
//             let key = format!("key1_{:05}", i).into_bytes();
//             let value = format!("value1_{:05}", i).into_bytes();
//             wal.append_current(&key, &value);
//             expected_data.push((key, value));
//         }
//         wal.freeze_current(); // This creates frozen1

//         // Write second batch (will be in frozen2)
//         for i in 0..1500 {
//             let key = format!("key2_{:05}", i).into_bytes();
//             let value = format!("value2_{:05}", i).into_bytes();
//             wal.append_current(&key, &value);
//             expected_data.push((key, value));
//         }
//         wal.freeze_current(); // This creates frozen2

//         // Write third batch (will be in active)
//         for i in 0..800 {
//             let key = format!("key3_{:05}", i).into_bytes();
//             let value = format!("value3_{:05}", i).into_bytes();
//             wal.append_current(&key, &value);
//             expected_data.push((key, value));
//         }

//         // Verify we have the expected number of files
//         assert_eq!(wal.frozen.len(), 2, "Should have 2 frozen WAL files");

//         // Now simulate a crash and recovery
//         drop(wal);
//         let wal = Wal::load(test_dir);
//         let memory = wal.replay();

//         // Verify all data is recovered correctly
//         for (key, value) in expected_data {
//             match memory.get(&key) {
//                 Some(Record::Value(bytes)) => {
//                     assert_eq!(
//                         bytes,
//                         Bytes::from(value),
//                         "Value mismatch for key {}",
//                         String::from_utf8_lossy(&key)
//                     );
//                 }
//                 _ => panic!(
//                     "Missing or invalid value for key {}",
//                     String::from_utf8_lossy(&key)
//                 ),
//             }
//         }

//         // Verify sizes
//         let guard = scc::ebr::Guard::new();
//         assert!(
//             memory
//                 .active_size
//                 .load(std::sync::atomic::Ordering::Relaxed)
//                 > 0,
//             "Active memtable should not be empty"
//         );
//         assert_eq!(memory.frozen.len(), 2, "Should have 2 frozen memtables");

//         // Print statistics
//         println!("WAL Recovery Test Statistics:");
//         println!(
//             "Active memtable size: {}",
//             memory
//                 .active_size
//                 .load(std::sync::atomic::Ordering::Relaxed)
//         );
//         println!("Number of frozen memtables: {}", memory.frozen.len());

//         let mut total_entries = 0;
//         for table in memory.frozen.iter(&guard) {
//             let entries = table.map.iter().count();
//             println!("Frozen memtable entries: {}", entries);
//             total_entries += entries;
//         }
//         let active_entries = memory.active.map.iter().count();
//         println!("Active memtable entries: {}", active_entries);
//         total_entries += active_entries;
//         println!("Total entries recovered: {}", total_entries);
//         assert_eq!(total_entries, 3300, "Total number of entries should match");

//         // tempdir will automatically clean up when it goes out of scope
//     }

//     #[test]
//     fn test_wal_recovery_with_overwrites() {
//         // Create test directory using tempdir
//         let temp = tempdir().unwrap();
//         let test_dir = temp.path().to_str().unwrap();
//         fs::create_dir_all(format!("{}/wal", test_dir)).unwrap();

//         // Create WAL and write data with overwrites
//         let mut wal = Wal::empty(test_dir.to_string());
//         let mut expected_data = std::collections::HashMap::new();

//         // Write first batch with some overwrites
//         for i in 0..500 {
//             let key = format!("key_{:03}", i % 200).into_bytes(); // Will cause overwrites
//             let value = format!("value1_{:03}", i).into_bytes();
//             wal.append_current(&key, &value);
//             expected_data.insert(key, value);
//         }
//         wal.freeze_current();

//         // Write second batch with more overwrites
//         for i in 0..300 {
//             let key = format!("key_{:03}", i % 150).into_bytes(); // More overwrites
//             let value = format!("value2_{:03}", i).into_bytes();
//             wal.append_current(&key, &value);
//             expected_data.insert(key, value);
//         }

//         // Simulate crash and recovery
//         drop(wal);
//         let wal = Wal::load(test_dir);
//         let memory = wal.replay();

//         // Get number of unique keys before consuming the HashMap
//         let unique_keys = expected_data.len();

//         // Verify final state is correct (latest values)
//         for (key, expected_value) in expected_data {
//             match memory.get(&key) {
//                 Some(Record::Value(bytes)) => {
//                     assert_eq!(
//                         bytes,
//                         Bytes::from(expected_value),
//                         "Value mismatch for key {}",
//                         String::from_utf8_lossy(&key)
//                     );
//                 }
//                 _ => panic!(
//                     "Missing or invalid value for key {}",
//                     String::from_utf8_lossy(&key)
//                 ),
//             }
//         }

//         // Print statistics
//         let guard = scc::ebr::Guard::new();
//         println!("\nWAL Recovery with Overwrites Test Statistics:");
//         println!(
//             "Active memtable size: {}",
//             memory
//                 .active_size
//                 .load(std::sync::atomic::Ordering::Relaxed)
//         );
//         println!("Number of frozen memtables: {}", memory.frozen.len());

//         let mut total_entries = 0;
//         for table in memory.frozen.iter(&guard) {
//             let entries = table.map.iter().count();
//             println!("Frozen memtable entries: {}", entries);
//             total_entries += entries;
//         }
//         let active_entries = memory.active.map.iter().count();
//         println!("Active memtable entries: {}", active_entries);
//         total_entries += active_entries;
//         println!("Total entries stored: {}", total_entries);
//         println!("Unique keys: {}", unique_keys);

//         // tempdir will automatically clean up when it goes out of scope
//     }
// }
