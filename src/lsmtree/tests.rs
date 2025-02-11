use super::*;
use crate::config::lsm::LsmConfig;
use crate::config::memory::MemoryConfig;
use crate::config::sst::SstConfig;
use crate::disk::disk::LsmDisk;
use crate::disk::sst::read::SstReader;
use crate::disk::sst::write::SstWriter;
use crate::lsmtree::signal::Signal;
use crate::lsmtree::tree::LsmTree;
use crate::memory::memory::LsmMemory;
use crate::memory::memtable::Memtable;
use bytes::Bytes;
use scc::ebr::Guard;
use scc::Queue;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

// ... existing code ...

#[test]
fn test_data_distribution_across_layers() {
    // Create temporary directory
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_str().unwrap().to_string();

    // Create LSM tree with relatively small thresholds to trigger freezing and flushing
    // let tree = LsmTree {
    //     mem: Arc::new(RwLock::new(LsmMemory {
    //         active: Arc::new(Memtable::new()),
    //         active_size: AtomicUsize::new(0),
    //         frozen: Queue::default(),
    //         frozen_sizes: Queue::default(),
    //     })),
    //     config: LsmConfig {
    //         path: dir_path.clone(),
    //         memory: MemoryConfig {
    //             freeze_size: 1024, // 1KB - small size to trigger freezing
    //             flush_size: 2048,  // 2KB - small size to trigger flushing
    //         },
    //         sst: SstConfig { block_size: 128 },
    //     },
    //     disk: LsmDisk::empty(dir_path),
    //     flush_signal: Arc::new(Signal::new()),
    //     flush_handle: std::thread::spawn(|| {}),
    // };

    let config = LsmConfig {
        path: dir_path.clone(),
        memory: MemoryConfig {
            freeze_size: 1024, // 1KB - small size to trigger freezing
            flush_size: 2048,  // 2KB - small size to trigger flushing
        },
        sst: SstConfig { block_size: 128 },
    };

    let tree = LsmTree::empty(config);

    // Insert enough data to trigger freezing and flushing
    for i in 0..1000 {
        let key = format!("key{:05}", i).into_bytes();
        let value = format!("value{:05}", i).into_bytes();
        tree.put(&key, &value);

        // Add small delay every 100 operations to allow background processes to work
        if i % 100 == 0 {
            thread::sleep(Duration::from_millis(10));
        }
    }

    // Force a final flush
    tree.flush();
    thread::sleep(Duration::from_millis(100)); // Give time for flush to complete

    // Verify data exists in memory (active memtable)
    let mem = tree.mem.read().unwrap();
    assert!(
        mem.active_size.load(Ordering::Relaxed) > 0,
        "Active memtable should contain data"
    );

    // Verify frozen memtables
    let guard = Guard::new();
    assert!(!mem.frozen.is_empty(), "Should have frozen memtables");

    // Verify data exists on disk
    // Sample some keys from different ranges
    let test_indices = [0, 100, 250, 500, 750, 999];
    for i in test_indices {
        let key = format!("key{:05}", i).into_bytes();
        let expected = format!("value{:05}", i);

        match tree.get(&key) {
            Some(value) => assert_eq!(
                value,
                Bytes::from(expected),
                "Value mismatch for key{:05}",
                i
            ),
            None => panic!("Key not found: key{:05}", i),
        }
    }

    // Verify data distribution across disk levels
    assert!(
        !tree.disk.level_0.is_empty()
            || !tree.disk.level_1.is_empty()
            || !tree.disk.level_2.is_empty(),
        "Data should be present in at least one disk level"
    );

    // Test deletion
    let key_to_delete = b"key00100";
    tree.delete(key_to_delete);
    assert_eq!(
        tree.get(key_to_delete),
        None,
        "Deleted key should not be found"
    );

    // Verify other keys still exist after deletion
    let key = b"key00101";
    assert!(
        tree.get(key).is_some(),
        "Non-deleted key should still exist"
    );
}
