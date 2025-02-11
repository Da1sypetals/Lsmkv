use crate::config::memory::MemoryConfig;
use crate::config::sst::SstConfig;
use crate::disk;
use crate::disk::disk::LsmDisk;
use crate::disk::sst::read::SstReader;
use crate::disk::sst::write::SstWriter;
use crate::memory::memory::LsmMemory;
use crate::memory::memtable::Memtable;
use crate::memory::record::Record;
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use tempfile::tempdir;

pub struct LsmTree {
    pub(crate) mem: Arc<RwLock<LsmMemory>>,
    pub(crate) mem_config: MemoryConfig,

    pub(crate) disk: LsmDisk,
}

impl LsmTree {
    pub fn put(&self, key: &[u8], value: &[u8]) {
        let current_size = {
            let mem = self.mem.read().unwrap();
            mem.put(key, value);

            mem.active_size.load(std::sync::atomic::Ordering::Relaxed)
        };

        if current_size > self.mem_config.freeze_size {
            let mut mem = self.mem.write().unwrap();
            mem.try_freeze_current(self.mem_config.freeze_size);
        }

        // todo: periodic flush to disk
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let mem = self.mem.read().unwrap();
        let mem_val = mem.get(key);
        if let Some(value) = mem_val {
            // data reside on memory
            return match value {
                Record::Value(bytes) => Some(bytes),
                Record::Tomb => None,
            };
        }

        // data probably reside on disk
        match self.disk.get(key) {
            Some(Record::Value(value)) => Some(value),
            _ => None,
        }
    }

    pub fn delete(&self, key: &[u8]) {
        let mem = self.mem.read().unwrap();
        mem.delete(key);
    }
}

// ........................................................................
// ................................. test .................................
// ........................................................................

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::sst::SstConfig;
    use crate::disk::sst::read::SstReader;
    use crate::disk::sst::write::SstWriter;
    use crate::memory::memtable::Memtable;
    use tempfile::tempdir;

    // Helper function to create a test tree
    fn create_test_tree() -> LsmTree {
        let config = MemoryConfig {
            freeze_size: 1024 * 1024, // 1MB
        };

        LsmTree {
            mem: Arc::new(RwLock::new(LsmMemory {
                active: Arc::new(Memtable::new()),
                active_size: AtomicUsize::new(0),
                frozen: VecDeque::new(),
            })),
            mem_config: config,
            disk: LsmDisk::empty("./".to_string()),
        }
    }

    #[test]
    /// Tests in-memory operations only (no disk persistence)
    fn test_inmem_basic_operations() {
        let tree = create_test_tree();

        // Test put and get
        tree.put(b"key1", b"value1");
        assert_eq!(tree.get(b"key1").unwrap(), Bytes::from("value1"));

        // Test overwrite
        tree.put(b"key1", b"value2");
        assert_eq!(tree.get(b"key1").unwrap(), Bytes::from("value2"));

        // Test get non-existent key
        assert_eq!(tree.get(b"nonexistent"), None);

        // Test delete
        tree.delete(b"key1");
        assert_eq!(tree.get(b"key1"), None);
    }

    #[test]
    /// Tests in-memory operations with multiple key-value pairs
    fn test_inmem_multiple_operations() {
        let tree = create_test_tree();

        // Insert multiple key-value pairs
        let test_data = vec![
            (b"key1", b"value1"),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
        ];

        for (key, value) in test_data.iter() {
            tree.put(*key, *value);
        }

        // Verify all values
        for (key, value) in test_data.iter() {
            assert_eq!(tree.get(*key).unwrap(), Bytes::copy_from_slice(*value));
        }

        // Delete some keys and verify
        tree.delete(b"key2");
        assert_eq!(tree.get(b"key2"), None);
        assert_eq!(tree.get(b"key1").unwrap(), Bytes::from("value1"));
        assert_eq!(tree.get(b"key3").unwrap(), Bytes::from("value3"));
    }

    #[test]
    /// Tests in-memory operations with empty values
    fn test_inmem_edge_cases() {
        let tree = create_test_tree();

        // Test empty value
        tree.put(b"empty", b"");
        assert_eq!(tree.get(b"empty").unwrap(), Bytes::from(""));

        // Test empty key
        tree.put(b"", b"empty_key");
        assert_eq!(tree.get(b"").unwrap(), Bytes::from("empty_key"));

        // Delete non-existent key (should not panic)
        tree.delete(b"nonexistent");
    }

    #[test]
    /// Tests in-memory operations with many pairs to trigger freezing
    fn test_inmem_freeze_trigger() {
        // this one
        let mut tree = create_test_tree();
        let small_freeze_size = 1000; // Small size to trigger freezes
        tree.mem_config.freeze_size = small_freeze_size;

        // Insert many key-value pairs to trigger freezes
        for i in 0..1000 {
            let key = format!("key-{}", i).into_bytes();
            let value = format!("value-{}", i).into_bytes();
            tree.put(&key, &value);
        }

        // Verify frozen state
        {
            let mem = tree.mem.read().unwrap();
            assert!(mem.frozen.len() > 0, "Should have frozen memtables");

            dbg!(mem.frozen.len());

            // Verify we can still read all values
            for i in 0..1000 {
                let key = format!("key-{}", i).into_bytes();
                let expected = format!("value-{}", i);
                assert_eq!(
                    mem.get(&key).unwrap(),
                    Record::Value(Bytes::from(expected)),
                    "Failed to read key-{}",
                    i
                );
            }
        }
    }

    #[test]
    /// Tests in-memory operations with high concurrent load to trigger freezing
    fn test_inmem_concurrent_freeze_trigger() {
        let mut tree = create_test_tree();
        let small_freeze_size = 1000; // Small size to trigger freezes
        tree.mem_config.freeze_size = small_freeze_size;
        let tree = Arc::new(tree);

        let num_threads = 10; // Number of concurrent threads
        let num_operations = 1000; // Number of operations per thread

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let tree = Arc::clone(&tree);
                std::thread::spawn(move || {
                    for i in 0..num_operations {
                        let key = format!("key-{}-{}", thread_id, i).into_bytes();
                        let value = format!("value-{}-{}", thread_id, i).into_bytes();

                        // Perform put operation
                        tree.put(&key, &value);

                        // Perform get operation to verify the value
                        let retrieved_value = tree.get(&key);
                        assert_eq!(
                            retrieved_value.unwrap(),
                            Bytes::from(value),
                            "Failed to read key-{}-{}",
                            thread_id,
                            i
                        );

                        // Perform delete operation
                        tree.delete(&key);

                        // Verify the key is deleted
                        assert_eq!(
                            tree.get(&key),
                            None,
                            "Key-{}-{} should be deleted",
                            thread_id,
                            i
                        );
                    }
                })
            })
            .collect();

        // Wait for all threads to finish
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify frozen state
        {
            let mem = tree.mem.read().unwrap();
            assert!(mem.frozen.len() > 0, "Should have frozen memtables");

            dbg!(mem.frozen.len());

            // Verify no keys remain in the active memtable
            for thread_id in 0..num_threads {
                for i in 0..num_operations {
                    let key = format!("key-{}-{}", thread_id, i).into_bytes();
                    assert!(
                        matches!(mem.get(&key).unwrap(), Record::Tomb),
                        "Key-{}-{} should be deleted after deletion",
                        thread_id,
                        i
                    );
                }
            }
        }
    }

    #[test]
    /// Tests concurrent writes with immediate reads from multiple threads
    fn test_inmem_concurrent_write_read() {
        let mut tree = create_test_tree();
        let small_freeze_size = 1000;
        tree.mem_config.freeze_size = small_freeze_size;
        let tree = Arc::new(tree);

        let num_threads = 10;
        let num_operations = 1000;

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let tree = Arc::clone(&tree);
                std::thread::spawn(move || {
                    // Each thread writes its own set of keys
                    for i in 0..num_operations {
                        let key = format!("t{}-key-{}", thread_id, i).into_bytes();
                        let value = format!("t{}-value-{}", thread_id, i).into_bytes();

                        // Write
                        tree.put(&key, &value);

                        // Immediate read verification
                        let result = tree.get(&key);
                        assert_eq!(
                            result.unwrap(),
                            Bytes::from(value.clone()),
                            "Thread {} failed immediate read of key {}",
                            thread_id,
                            i
                        );
                    }
                })
            })
            .collect();

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Final verification that all values are present
        {
            let mem = tree.mem.read().unwrap();
            println!("Number of frozen memtables: {}", mem.frozen.len());

            for thread_id in 0..num_threads {
                for i in 0..num_operations {
                    let key = format!("t{}-key-{}", thread_id, i).into_bytes();
                    let expected = format!("t{}-value-{}", thread_id, i);
                    let result = mem.get(&key);
                    assert_eq!(
                        result.unwrap(),
                        Record::Value(Bytes::from(expected)),
                        "Thread {} key {} not found in final verification",
                        thread_id,
                        i
                    );
                }
            }
        }
    }

    #[test]
    fn test_lsmtree_with_prefilled_components() {
        // Create temporary directory for disk component
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();

        // Create disk component
        let mut disk = LsmDisk::empty(dir_path.clone());

        // Level 0 SST
        let level0_memtable = Arc::new(Memtable::new());
        level0_memtable.put(b"disk0_key1", b"disk0_value1");
        level0_memtable.put(b"disk0_key2", b"disk0_value2");
        let sst_writer = SstWriter::new(
            SstConfig { block_size: 4096 },
            dir_path.clone(),
            "sst1".to_string(),
            level0_memtable,
        );
        sst_writer.build();
        disk.level_0.push(SstReader {
            dir: dir_path.clone(),
            filename: "sst1".to_string(),
        });

        // Level 1 SST
        let level1_memtable = Arc::new(Memtable::new());
        level1_memtable.put(b"disk1_key1", b"disk1_value1");
        level1_memtable.put(b"disk1_key2", b"disk1_value2");
        let sst_writer = SstWriter::new(
            SstConfig { block_size: 4096 },
            dir_path.clone(),
            "sst2".to_string(),
            level1_memtable,
        );
        sst_writer.build();
        disk.level_1.push(SstReader {
            dir: dir_path.clone(),
            filename: "sst2".to_string(),
        });

        // Level 2 SST with shared key
        let level2_memtable = Arc::new(Memtable::new());
        level2_memtable.put(b"disk2_key1", b"disk2_value1");
        level2_memtable.put(b"disk2_key2", b"disk2_value2");
        level2_memtable.put(b"shared_key", b"disk_value"); // Will be overridden by memory
        let sst_writer = SstWriter::new(
            SstConfig { block_size: 4096 },
            dir_path.clone(),
            "sst3".to_string(),
            level2_memtable,
        );
        sst_writer.build();
        disk.level_2.push(SstReader {
            dir: dir_path.clone(),
            filename: "sst3".to_string(),
        });

        // Create memory component with active memtable
        let active_memtable = Arc::new(Memtable::new());
        active_memtable.put(b"mem_key1", b"mem_value1");
        active_memtable.put(b"mem_key2", b"mem_value2");
        active_memtable.put(b"shared_key", b"mem_value"); // Will override disk value

        // Calculate actual size of active memtable
        let active_size = b"mem_key1".len()
            + b"mem_value1".len()
            + b"mem_key2".len()
            + b"mem_value2".len()
            + b"shared_key".len()
            + b"mem_value".len();

        // Create frozen memtables
        let frozen_memtable1 = Arc::new(Memtable::new());
        frozen_memtable1.put(b"frozen1_key1", b"frozen1_value1");
        frozen_memtable1.put(b"frozen1_key2", b"frozen1_value2");

        let frozen_memtable2 = Arc::new(Memtable::new());
        frozen_memtable2.put(b"frozen2_key1", b"frozen2_value1");
        frozen_memtable2.put(b"frozen2_key2", b"frozen2_value2");
        frozen_memtable2.put(b"shared_key", b"frozen_share_value");

        let mut frozen = VecDeque::new();
        frozen.push_back(frozen_memtable1);
        frozen.push_back(frozen_memtable2);

        let mem = LsmMemory {
            active: active_memtable,
            active_size: AtomicUsize::new(active_size),
            frozen,
        };

        // Create LSM tree with prefilled components
        let tree = LsmTree {
            mem: Arc::new(RwLock::new(mem)),
            mem_config: MemoryConfig { freeze_size: 1000 },
            disk,
        };

        // Test retrieving from active memtable
        assert_eq!(tree.get(b"mem_key1").unwrap(), Bytes::from("mem_value1"));
        assert_eq!(tree.get(b"mem_key2").unwrap(), Bytes::from("mem_value2"));

        // Test retrieving from frozen memtables
        assert_eq!(
            tree.get(b"frozen1_key1").unwrap(),
            Bytes::from("frozen1_value1")
        );
        assert_eq!(
            tree.get(b"frozen1_key2").unwrap(),
            Bytes::from("frozen1_value2")
        );
        assert_eq!(
            tree.get(b"frozen2_key1").unwrap(),
            Bytes::from("frozen2_value1")
        );
        assert_eq!(
            tree.get(b"frozen2_key2").unwrap(),
            Bytes::from("frozen2_value2")
        );

        // Test retrieving from disk at different levels
        assert_eq!(
            tree.get(b"disk0_key1").unwrap(),
            Bytes::from("disk0_value1")
        );
        assert_eq!(
            tree.get(b"disk0_key2").unwrap(),
            Bytes::from("disk0_value2")
        );
        assert_eq!(
            tree.get(b"disk1_key1").unwrap(),
            Bytes::from("disk1_value1")
        );
        assert_eq!(
            tree.get(b"disk1_key2").unwrap(),
            Bytes::from("disk1_value2")
        );
        assert_eq!(
            tree.get(b"disk2_key1").unwrap(),
            Bytes::from("disk2_value1")
        );
        assert_eq!(
            tree.get(b"disk2_key2").unwrap(),
            Bytes::from("disk2_value2")
        );

        // Test that memory value overrides disk value
        assert_eq!(tree.get(b"shared_key").unwrap(), Bytes::from("mem_value"));

        // Test deleting from different components
        tree.delete(b"mem_key1");
        assert_eq!(tree.get(b"mem_key1"), None);

        tree.delete(b"frozen1_key1");
        assert_eq!(tree.get(b"frozen1_key1"), None);

        tree.delete(b"disk0_key1");
        assert_eq!(tree.get(b"disk0_key1"), None);
    }

    #[test]
    fn test_lsmtree_large_datasets() {
        // Create temporary directory for disk component
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();

        // Create disk component
        let mut disk = LsmDisk::empty(dir_path.clone());

        // Level 0 SST - 10,000 entries
        let level0_memtable = Arc::new(Memtable::new());
        for i in 0..10_000 {
            let key = format!("disk0_key{:05}", i).into_bytes();
            let value = format!("disk0_value{:05}", i).into_bytes();
            level0_memtable.put(&key, &value);
        }
        // Add some shared keys that will be overridden by memory
        level0_memtable.put(b"shared_key1", b"disk0_shared_value");
        level0_memtable.put(b"shared_key2", b"disk0_shared_value");

        let sst_writer = SstWriter::new(
            SstConfig { block_size: 4096 },
            dir_path.clone(),
            "sst1".to_string(),
            level0_memtable,
        );
        sst_writer.build();
        disk.level_0.push(SstReader {
            dir: dir_path.clone(),
            filename: "sst1".to_string(),
        });

        // Level 1 SST - 20,000 entries
        let level1_memtable = Arc::new(Memtable::new());
        for i in 0..20_000 {
            let key = format!("disk1_key{:05}", i).into_bytes();
            let value = format!("disk1_value{:05}", i).into_bytes();
            level1_memtable.put(&key, &value);
        }
        level1_memtable.put(b"shared_key3", b"disk1_shared_value");
        level1_memtable.put(b"shared_key4", b"disk1_shared_value");

        let sst_writer = SstWriter::new(
            SstConfig { block_size: 4096 },
            dir_path.clone(),
            "sst2".to_string(),
            level1_memtable,
        );
        sst_writer.build();
        disk.level_1.push(SstReader {
            dir: dir_path.clone(),
            filename: "sst2".to_string(),
        });

        // Level 2 SST - 40,000 entries
        let level2_memtable = Arc::new(Memtable::new());
        for i in 0..40_000 {
            let key = format!("disk2_key{:05}", i).into_bytes();
            let value = format!("disk2_value{:05}", i).into_bytes();
            level2_memtable.put(&key, &value);
        }
        level2_memtable.put(b"shared_key5", b"disk2_shared_value");
        level2_memtable.put(b"shared_key6", b"disk2_shared_value");

        let sst_writer = SstWriter::new(
            SstConfig { block_size: 4096 },
            dir_path.clone(),
            "sst3".to_string(),
            level2_memtable,
        );
        sst_writer.build();
        disk.level_2.push(SstReader {
            dir: dir_path.clone(),
            filename: "sst3".to_string(),
        });

        // Create memory component with active memtable - 5,000 entries
        let active_memtable = Arc::new(Memtable::new());
        let mut active_size = 0;
        for i in 0..5_000 {
            let key = format!("mem_key{:05}", i).into_bytes();
            let value = format!("mem_value{:05}", i).into_bytes();
            active_memtable.put(&key, &value);
            active_size += key.len() + value.len();
        }
        // Override some disk values
        for shared_key in &[
            "shared_key1",
            "shared_key2",
            "shared_key3",
            "shared_key4",
            "shared_key5",
            "shared_key6",
        ] {
            let value = format!("mem_override_{}", shared_key).into_bytes();
            active_memtable.put(shared_key.as_bytes(), &value);
            active_size += shared_key.len() + value.len();
        }

        // Create frozen memtables - 8,000 entries each
        let frozen_memtable1 = Arc::new(Memtable::new());
        for i in 0..8_000 {
            let key = format!("frozen1_key{:05}", i).into_bytes();
            let value = format!("frozen1_value{:05}", i).into_bytes();
            frozen_memtable1.put(&key, &value);
        }

        let frozen_memtable2 = Arc::new(Memtable::new());
        for i in 0..8_000 {
            let key = format!("frozen2_key{:05}", i).into_bytes();
            let value = format!("frozen2_value{:05}", i).into_bytes();
            frozen_memtable2.put(&key, &value);
        }

        let mut frozen = VecDeque::new();
        frozen.push_back(frozen_memtable1);
        frozen.push_back(frozen_memtable2);

        let mem = LsmMemory {
            active: active_memtable,
            active_size: AtomicUsize::new(active_size),
            frozen,
        };

        // Create LSM tree with prefilled components
        let tree = LsmTree {
            mem: Arc::new(RwLock::new(mem)),
            mem_config: MemoryConfig {
                freeze_size: 10 * 1024 * 1024,
            }, // 10MB
            disk,
        };

        // Test retrieving from active memtable (sample)
        for i in (0..5_000).step_by(500) {
            let key = format!("mem_key{:05}", i).into_bytes();
            let expected = format!("mem_value{:05}", i);
            assert_eq!(tree.get(&key).unwrap(), Bytes::from(expected));
        }

        // Test retrieving from frozen memtables (sample)
        for i in (0..8_000).step_by(800) {
            let key = format!("frozen1_key{:05}", i).into_bytes();
            let expected = format!("frozen1_value{:05}", i);
            assert_eq!(tree.get(&key).unwrap(), Bytes::from(expected));

            let key = format!("frozen2_key{:05}", i).into_bytes();
            let expected = format!("frozen2_value{:05}", i);
            assert_eq!(tree.get(&key).unwrap(), Bytes::from(expected));
        }

        // Test retrieving from disk at different levels (sample)
        for i in (0..10_000).step_by(1000) {
            let key = format!("disk0_key{:05}", i).into_bytes();
            let expected = format!("disk0_value{:05}", i);
            assert_eq!(tree.get(&key).unwrap(), Bytes::from(expected));
        }

        for i in (0..20_000).step_by(2000) {
            let key = format!("disk1_key{:05}", i).into_bytes();
            let expected = format!("disk1_value{:05}", i);
            assert_eq!(tree.get(&key).unwrap(), Bytes::from(expected));
        }

        for i in (0..40_000).step_by(4000) {
            let key = format!("disk2_key{:05}", i).into_bytes();
            let expected = format!("disk2_value{:05}", i);
            assert_eq!(tree.get(&key).unwrap(), Bytes::from(expected));
        }

        // Test that memory values override disk values
        for shared_key in &[
            "shared_key1",
            "shared_key2",
            "shared_key3",
            "shared_key4",
            "shared_key5",
            "shared_key6",
        ] {
            let expected = format!("mem_override_{}", shared_key);
            assert_eq!(
                tree.get(shared_key.as_bytes()).unwrap(),
                Bytes::from(expected)
            );
        }

        // Test deleting from different components (sample)
        // Delete from memory
        let key = b"mem_key00100";
        tree.delete(key);
        assert_eq!(tree.get(key), None);

        // Delete from frozen
        let key = b"frozen1_key00100";
        tree.delete(key);
        assert_eq!(tree.get(key), None);

        // Delete from disk
        let key = b"disk0_key00100";
        tree.delete(key);
        assert_eq!(tree.get(key), None);

        // Verify other entries still exist after deletions
        assert_eq!(
            tree.get(b"mem_key00101").unwrap(),
            Bytes::from("mem_value00101")
        );
        assert_eq!(
            tree.get(b"frozen1_key00101").unwrap(),
            Bytes::from("frozen1_value00101")
        );
        assert_eq!(
            tree.get(b"disk0_key00101").unwrap(),
            Bytes::from("disk0_value00101")
        );
    }
}
