use crate::{
    config::{disk::DiskConfig, lsm::LsmConfig, memory::MemoryConfig, sst::SstConfig},
    lsmtree::tree::LsmTree,
};
use bytes::Bytes;
use rand::seq::IteratorRandom;
use std::{thread, time::Duration};
use tempfile::tempdir;

#[test]
fn test_basic_0() {
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_str().unwrap().to_string();

    let config = LsmConfig {
        dir: dir_path.clone(),
        disk: DiskConfig {
            level_0_size_threshold: 65536,
            block_size_multiplier: 1,
            level_0_threshold: 32,
            level_1_threshold: 32,
            level_2_threshold: 100000,
            auto_compact: true,
        },
        memory: MemoryConfig {
            freeze_size: 1024,
            flush_size: 16384,
        },
        sst: SstConfig {
            block_size: 128,
            scale: 100,
            fpr: 0.01,
        },
    };

    let mut tree = LsmTree::empty(config);

    let n_entries = 10000;

    // Insert enough data to trigger freezing and flushing
    for i in 0..n_entries {
        let key = format!("key{:05}", i).into_bytes();
        let value = format!("value{:05}", i).into_bytes();
        tree.put(&key, &value);
    }

    // Force a final flush
    // tree.persist();

    thread::sleep(Duration::from_millis(100)); // Give time for flush to complete

    // Verify data exists in memory (active memtable)
    let mem = tree.mem.read().unwrap();
    assert!(
        mem.active_size.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "Active memtable should contain data"
    );

    // assert!(!mem.frozen.is_empty(), "Should have frozen memtables");

    // format these debugs into beautiful report of occupation of each level
    let report = format!(
        "Memory: active size: {}, # frozen tables: {}\nDisk: level_0: {}, level_1: {}, level_2: {}",
        mem.active_size.load(std::sync::atomic::Ordering::Relaxed),
        mem.frozen.len(),
        tree.disk.level_0.read().unwrap().sst_readers.len(),
        tree.disk.level_1.read().unwrap().sst_readers.len(),
        tree.disk.level_2.read().unwrap().sst_readers.len()
    );
    println!("{}", report);

    // Verify data exists on disk
    let n_samples = 100;
    // Sample some keys from different ranges
    let mut test_indices = (0..n_entries).choose_multiple(&mut rand::rng(), n_samples);
    test_indices.reverse();
    for i in test_indices {
        let key = format!("key{:05}", i).into_bytes();
        let expected = format!("value{:05}", i);

        match tree.get(&key) {
            Some(value) => {
                assert_eq!(
                    value,
                    Bytes::from(expected),
                    "Value mismatch for key{:05}",
                    i
                );
                println!("Value matched for key{:05}", i);
            }
            None => panic!("Key not found: key{:05}", i),
        }
    }

    // Test deletion
    // delete all key modulo 100 = 0
    for i in 0..n_entries {
        if i % 100 == 0 {
            let key = format!("key{:05}", i).into_bytes();
            tree.delete(&key);
        }
    }

    // examine deletion via get
    for i in 0..n_entries {
        if i % 100 == 0 {
            let key = format!("key{:05}", i).into_bytes();
            assert!(tree.get(&key).is_none(), "Key should be deleted");
        }
    }

    // Verify 100 other random keys still exist after deletion
    for i in (0..n_entries).choose_multiple(&mut rand::rng(), 100) {
        if i % 100 != 0 {
            let key = format!("key{:05}", i).into_bytes();
            assert!(tree.get(&key).is_some(), "Key should still exist");
        }
    }
}
