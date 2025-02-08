use crate::memory::{config::LsmMemoryConfig, memory::LsmMemory};
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

pub struct LsmTree {
    pub(crate) mem: Arc<RwLock<LsmMemory>>,
    pub(crate) mem_config: LsmMemoryConfig,
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
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let mem = self.mem.read().unwrap();
        mem.get(key)

        // todo: search on disk
    }

    pub fn delete(&self, key: &[u8]) {
        let mem = self.mem.read().unwrap();
        mem.delete(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{config::LsmMemoryConfig, memtable::Memtable};

    // Helper function to create a test tree
    fn create_test_tree() -> LsmTree {
        let config = LsmMemoryConfig {
            freeze_size: 1024 * 1024, // 1MB
        };
        LsmTree {
            mem: Arc::new(RwLock::new(LsmMemory {
                active: Arc::new(Memtable::new()),
                active_size: AtomicUsize::new(0),
                frozen: VecDeque::new(),
            })),
            mem_config: config,
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
                    Bytes::from(expected),
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
                    assert_eq!(
                        mem.get(&key),
                        None,
                        "Key-{}-{} should not exist after deletion",
                        thread_id,
                        i
                    );
                }
            }
        }
    }
}
