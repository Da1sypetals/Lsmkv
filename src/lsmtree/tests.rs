use crate::config::disk::DiskConfig;
use crate::config::lsm::LsmConfig;
use crate::config::memory::MemoryConfig;
use crate::config::sst::SstConfig;
use crate::lsmtree::tree::LsmTree;
use bytes::Bytes;
use rand::seq::IteratorRandom;
use rand::Rng;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::{fs, thread};
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
        dir: dir_path.clone(),
        disk: DiskConfig {
            level_0_size_threshold: 65536,
            block_size_multiplier: 1,
            level_0_threshold: 32,
            level_1_threshold: 32,
            level_2_threshold: 1000000000000000,
            // currently not used
            auto_compact: true,
        },
        memory: MemoryConfig {
            freeze_size: 1024, // 1KB - small size to trigger freezing
            flush_size: 16384, // 2KB - small size to trigger flushing
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

        // Add small delay every 100 operations to allow background processes to work
        if i % 100 == 0 {
            thread::sleep(Duration::from_millis(10));
        }
    }

    // Force a final flush
    // tree.persist();

    thread::sleep(Duration::from_millis(100)); // Give time for flush to complete

    // Verify data exists in memory (active memtable)
    let mem = tree.mem.read().unwrap();
    assert!(
        mem.active_size.load(Ordering::Relaxed) > 0,
        "Active memtable should contain data"
    );

    // assert!(!mem.frozen.is_empty(), "Should have frozen memtables");

    // format these debugs into beautiful report of occupation of each level
    let report = format!(
        "Memory: active size: {}, # frozen tables: {}\nDisk: level_0: {}, level_1: {}, level_2: {}",
        mem.active_size.load(Ordering::Relaxed),
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
        !tree.disk.level_0.read().unwrap().sst_readers.is_empty()
            || !tree.disk.level_1.read().unwrap().sst_readers.is_empty()
            || !tree.disk.level_2.read().unwrap().sst_readers.is_empty(),
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

/*

#[test]
fn test_concurrent_large_dataset() {
    use std::sync::atomic::AtomicBool;
    use std::time::Instant;

    // Create temporary directory for LSM tree
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_str().unwrap().to_string();

    // Configure LSM tree with more realistic sizes for large dataset
    let config = LsmConfig {
        dir: dir_path.clone(),
        disk: DiskConfig {
            level_0_size_threshold: 65536,
            block_size_multiplier: 1,
            level_0_threshold: 1000000000000000,
            level_1_threshold: 1000000000000000,
            level_2_threshold: 1000000000000000,
        },
        memory: MemoryConfig {
            freeze_size: 4 * 1024 * 1024, // 4MB - reasonable size for memtable
            flush_size: 16 * 1024 * 1024, // 16MB - trigger flush after multiple freezes
        },
        sst: SstConfig {
            block_size: 4096, // 4KB blocks, typical page size
        },
    };

    let tree = Arc::new(LsmTree::empty(config));
    let running = Arc::new(AtomicBool::new(true));
    let start_time = Instant::now();

    // Number of operations each writer will perform
    let ops_per_writer = 10_000;
    let n_writers = 4;
    let n_readers = 2;
    let total_ops = ops_per_writer * n_writers;

    // Track successful operations
    let successful_writes = Arc::new(AtomicUsize::new(0));
    let successful_reads = Arc::new(AtomicUsize::new(0));

    // Spawn writer threads
    let mut handles = vec![];
    for writer_id in 0..n_writers {
        let tree = Arc::clone(&tree);
        let running = Arc::clone(&running);
        let successful_writes = Arc::clone(&successful_writes);

        let handle = thread::spawn(move || {
            let mut rng = rand::rng();
            let start = writer_id * ops_per_writer;
            let end = start + ops_per_writer;

            for i in start..end {
                if !running.load(Ordering::Relaxed) {
                    break;
                }

                // Generate key-value pair
                let key = format!("key{:010}", i).into_bytes();
                let value = format!("value{:010}-from-writer-{}", i, writer_id).into_bytes();

                // Randomly choose between put and delete (90% puts, 10% deletes)
                if rng.random_ratio(9, 10) {
                    tree.put(&key, &value);
                } else {
                    tree.delete(&key);
                }

                successful_writes.fetch_add(1, Ordering::Relaxed);

                // Small sleep every 1000 ops to prevent overwhelming the system
                if i % 1000 == 0 {
                    thread::sleep(Duration::from_micros(100));
                }
            }
        });
        handles.push(handle);
    }

    // Spawn reader threads that continuously verify data
    for _ in 0..n_readers {
        let tree = Arc::clone(&tree);
        let running = Arc::clone(&running);
        let successful_reads = Arc::clone(&successful_reads);

        let handle = thread::spawn(move || {
            let mut rng = rand::rng();

            while running.load(Ordering::Relaxed) {
                // Randomly select a key to read
                let i: usize = rng.random_range(0..total_ops);
                let key = format!("key{:010}", i).into_bytes();

                if let Some(_) = tree.get(&key) {
                    successful_reads.fetch_add(1, Ordering::Relaxed);
                }

                // Small sleep between reads
                thread::sleep(Duration::from_micros(100));
            }
        });
        handles.push(handle);
    }

    // Let the test run for a reasonable duration
    thread::sleep(Duration::from_secs(30));
    running.store(false, Ordering::Relaxed);

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Final statistics
    let elapsed = start_time.elapsed();
    let writes = successful_writes.load(Ordering::Relaxed);
    let reads = successful_reads.load(Ordering::Relaxed);

    println!("\nConcurrent Test Results:");
    println!("------------------------");
    println!("Test duration: {:.2} seconds", elapsed.as_secs_f64());
    println!("Total successful writes: {}", writes);
    println!("Total successful reads: {}", reads);
    println!(
        "Write throughput: {:.2} ops/sec",
        writes as f64 / elapsed.as_secs_f64()
    );
    println!(
        "Read throughput: {:.2} ops/sec",
        reads as f64 / elapsed.as_secs_f64()
    );

    // Verify final state
    let mem = tree.mem.read().unwrap();
    println!("\nFinal LSM Tree State:");
    println!("------------------------");
    println!(
        "Memory: active size: {}MB, # frozen tables: {}",
        mem.active_size.load(Ordering::Relaxed) / 1024 / 1024,
        mem.frozen.len()
    );
    println!(
        "Disk: level_0: {}, level_1: {}, level_2: {}",
        tree.disk.level_0.read().unwrap().sst_readers.len(),
        tree.disk.level_1.read().unwrap().sst_readers.len(),
        tree.disk.level_2.read().unwrap().sst_readers.len()
    );

    // Verify data consistency by sampling some keys
    let mut rng = rand::rng();
    let sample_size = 1000;
    let mut verification_success = 0;

    for _ in 0..sample_size {
        let i: usize = rng.random_range(0..total_ops);
        let key = format!("key{:010}", i).into_bytes();

        if let Some(_) = tree.get(&key) {
            verification_success += 1;
        }
    }

    println!("\nData Verification:");
    println!("------------------------");
    println!(
        "Sampled {} random keys, found {} valid entries",
        sample_size, verification_success
    );

    // The test is considered successful if we achieved significant throughput
    // and maintained data consistency
    assert!(
        writes > total_ops / 2,
        "Should complete at least 50% of writes"
    );
    assert!(reads > 0, "Should complete some successful reads");
    assert!(
        verification_success > 0,
        "Should find some valid entries in final verification"
    );
}

*/

#[test]
fn test_deterministic_concurrent_operations() {
    use std::collections::HashMap;
    use std::time::Instant;

    println!("\n=== Starting Deterministic Concurrent Test ===");

    // Create temporary directory for LSM tree
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_str().unwrap().to_string();

    println!("1. Configuring LSM Tree...");
    let config = LsmConfig {
        dir: dir_path.clone(),
        disk: DiskConfig {
            level_0_size_threshold: 65536,
            block_size_multiplier: 1,
            level_0_threshold: 32,
            level_1_threshold: 32,
            level_2_threshold: 1000000000000000,
            // currently not used
            auto_compact: true,
        },
        memory: MemoryConfig {
            freeze_size: 1024 * 1024,    // 1MB - smaller for more frequent freezes
            flush_size: 4 * 1024 * 1024, // 4MB - trigger flush after multiple freezes
        },
        sst: SstConfig {
            block_size: 4096,
            scale: 100,
            fpr: 0.01,
        },
    };

    let tree = Arc::new(LsmTree::empty(config));

    // Track the expected state
    let expected_state = Arc::new(RwLock::new(HashMap::new()));

    // Define deterministic workload parameters
    let n_writers = 10;
    let ops_per_writer = 10_000; // Smaller than random test for deterministic verification
    let total_ops = ops_per_writer * n_writers;

    // Pre-fill the expected state
    for i in 0..total_ops {
        let key = format!("key{:010}", i).into_bytes();
        let value = format!("value{:010}-writer-{}", i, i / ops_per_writer).into_bytes();

        if i % 10 == 0 && i >= 3 {
            // Delete previous 3 entries
            for j in (i - 3)..i {
                let del_key = format!("key{:010}", j).into_bytes();
                expected_state.write().unwrap().remove(&del_key);
            }
        } else if i % 2 == 0 {
            // Insert on even numbers
            expected_state.write().unwrap().insert(key, value);
        }
    }

    println!(
        "2. Launching {} writer threads, {} ops each...",
        n_writers, ops_per_writer
    );

    // Spawn writer threads with deterministic patterns
    let mut write_handles = vec![];
    let start_time = Instant::now();

    for writer_id in 0..n_writers {
        let tree = Arc::clone(&tree);
        let expected_state = Arc::clone(&expected_state);

        let handle = thread::spawn(move || {
            let start = writer_id * ops_per_writer;
            let end = start + ops_per_writer;

            println!(
                "   Writer {} starting operations {} to {}",
                writer_id,
                start,
                end - 1
            );

            for i in start..end {
                // Deterministic pattern:
                // - Even numbers: insert
                // - Every 10th: delete previous 3 entries
                // - Every 100th: print progress

                let key = format!("key{:010}", i).into_bytes();
                let value = format!("value{:010}-writer-{}", i, writer_id).into_bytes();

                if i % 100 == 0 {
                    println!("   Writer {} at operation {}", writer_id, i);
                }

                if i % 10 == 0 && i >= 3 {
                    // Delete previous 3 entries
                    for j in (i - 3)..i {
                        let del_key = format!("key{:010}", j).into_bytes();
                        tree.delete(&del_key);
                    }
                } else if i % 2 == 0 {
                    // Insert on even numbers
                    tree.put(&key, &value);
                }
            }
            println!("   Writer {} completed", writer_id);
        });
        write_handles.push(handle);
    }

    // Wait for all writers to complete
    println!("3. Waiting for writers to complete...");
    for handle in write_handles {
        handle.join().unwrap();
    }
    let write_duration = start_time.elapsed();
    println!(
        "   All writers completed in {:.2} seconds",
        write_duration.as_secs_f64()
    );

    // Give some time for background compaction to settle
    println!("4. Allowing background operations to settle...");
    thread::sleep(Duration::from_millis(500));

    println!("5. Starting verification...");
    let verify_start = Instant::now();

    // Final state verification
    let mem = tree.mem.read().unwrap();
    println!("\nLSM Tree State:");
    println!("------------------------");
    println!("Memory:");
    println!("  Active size: {}", mem.active_size.load(Ordering::Relaxed));
    println!("  Frozen tables: {}", mem.frozen.len());
    println!("Disk levels:");
    println!(
        "  Level 0: {}",
        tree.disk.level_0.read().unwrap().sst_readers.len()
    );
    println!(
        "  Level 1: {}",
        tree.disk.level_1.read().unwrap().sst_readers.len()
    );
    println!(
        "  Level 2: {}",
        tree.disk.level_2.read().unwrap().sst_readers.len()
    );

    // Verify all expected entries
    let expected = expected_state.read().unwrap();
    println!("\nData Verification:");
    println!("------------------------");
    println!("Expected entries: {}", expected.len());

    let mut verified = 0;
    let mut errors = 0;
    let mut missing = 0;
    let mut mismatches = 0;

    for (key, expected_value) in expected.iter() {
        match tree.get(key) {
            Some(actual_value) => {
                if actual_value == Bytes::from(expected_value.clone()) {
                    verified += 1;
                } else {
                    mismatches += 1;
                    errors += 1;
                    println!("Value mismatch for key: {:?}", String::from_utf8_lossy(key));
                    println!("  Expected: {:?}", String::from_utf8_lossy(expected_value));
                    println!("  Got: {:?}", String::from_utf8_lossy(&actual_value));
                }
            }
            None => {
                missing += 1;
                errors += 1;
                println!("Missing key: {:?}", String::from_utf8_lossy(key));
            }
        }

        if (verified + errors) % 1000 == 0 {
            println!("Progress: {}/10000 entries verified", verified + errors);
        }
    }

    let verify_duration = verify_start.elapsed();

    println!("\nVerification Results:");
    println!("------------------------");
    println!("Total entries: {}", expected.len());
    println!("Successfully verified: {}", verified);
    println!("Missing entries: {}", missing);
    println!("Value mismatches: {}", mismatches);
    println!("Total errors: {}", errors);
    println!(
        "Verification time: {:.2} seconds",
        verify_duration.as_secs_f64()
    );
    println!(
        "Total test time: {:.2} seconds",
        start_time.elapsed().as_secs_f64()
    );

    assert_eq!(errors, 0, "Found {} errors in verification", errors);
    assert!(verified > 0, "No entries were verified");

    println!("\n=== Deterministic Concurrent Test Completed Successfully ===");
}

#[test]
fn test_simple() {
    use std::collections::HashMap;
    use std::time::Instant;

    println!("\n=== Starting Serial Insert Test ===");

    // Create temporary directory for LSM tree
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_str().unwrap().to_string();

    println!("1. Configuring LSM Tree...");
    let config = LsmConfig {
        dir: dir_path.clone(),
        disk: DiskConfig {
            level_0_size_threshold: 1024,
            block_size_multiplier: 8,
            level_0_threshold: 16,
            level_1_threshold: 128,
            level_2_threshold: 1024,
            // currently not used
            auto_compact: true,
        },
        memory: MemoryConfig {
            freeze_size: 1024,    // 1KB - smaller for more frequent freezes
            flush_size: 4 * 1024, // 4KB
        },
        sst: SstConfig {
            block_size: 4096,
            scale: 100,
            fpr: 0.01,
        },
    };

    let tree = LsmTree::empty(config);

    // Track the expected state
    let mut expected_state = HashMap::new();

    // Define test parameters
    let n_keys = 100_000;
    println!("2. Starting serial insertion of {} keys...", n_keys);
    let start_time = Instant::now();

    // Insert keys sequentially
    for i in 0..n_keys {
        let key = format!("key{:05}", i).into_bytes();
        let value = format!("value{:05}", i).into_bytes();

        if i % 1000 == 0 {
            println!("Progress: {}/{} keys inserted", i, n_keys);
        }

        tree.put(&key, &value);
        expected_state.insert(key, value);

        // Small sleep every 10k operations to allow background processes to work
    }

    // Print final LSM tree state
    let mem = tree.mem.read().unwrap();
    println!("\nFinal LSM Tree State:");
    println!("------------------------");
    println!("Memory:");
    println!(
        "  Active size: {}",
        mem.active_size.load(std::sync::atomic::Ordering::Relaxed)
    );
    println!("  Frozen tables: {}", mem.frozen.len());
    println!("Disk levels:");
    println!(
        "  Level 0: {}",
        tree.disk.level_0.read().unwrap().sst_readers.len()
    );
    println!(
        "  Level 1: {}",
        tree.disk.level_1.read().unwrap().sst_readers.len()
    );
    println!(
        "  Level 2: {}",
        tree.disk.level_2.read().unwrap().sst_readers.len()
    );

    let res = tree.get(b"key00000");
    println!("res: {:?}", res);
}

#[test]
fn test_serial_insert_versions() {
    use std::collections::HashMap;
    use std::time::Instant;

    println!("\n=== Starting Serial Version Insert Test ===");

    // Create temporary directory for LSM tree
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_str().unwrap().to_string();

    println!("1. Configuring LSM Tree...");
    let config = LsmConfig {
        dir: dir_path.clone(),
        disk: DiskConfig {
            level_0_size_threshold: 65536,
            block_size_multiplier: 4,
            level_0_threshold: 16,
            level_1_threshold: 16,
            level_2_threshold: 16,
            // currently not used
            auto_compact: false,
        },
        memory: MemoryConfig {
            freeze_size: 1024,    // 1KB - smaller for more frequent freezes
            flush_size: 4 * 1024, // 4KB
        },
        sst: SstConfig {
            block_size: 4096,
            scale: 100,
            fpr: 0.01,
        },
    };

    let tree = LsmTree::empty(config);

    // Define test parameters
    let n_keys = 10000; // Small number of keys
    let n_versions = 5; // Number of versions per key
    println!(
        "2. Starting serial insertion of {} keys with {} versions each...",
        n_keys, n_versions
    );
    let start_time = Instant::now();

    // Insert multiple versions for each key
    for version in 0..n_versions {
        for i in 0..n_keys {
            let key = format!("key{:05}", i);
            let value = format!("value{:05}_v{}", i, version);

            if i % 1000 == 0 && version == 0 {
                println!("Progress: {}/{} keys, version {}", i, n_keys, version);
            }

            tree.put(key.as_bytes(), value.as_bytes());
        }
        println!("Completed version {} insertion", version);
        thread::sleep(Duration::from_millis(10)); // Allow some time between versions
    }

    let insert_duration = start_time.elapsed();
    println!(
        "\nInsertion completed in {:.2} seconds",
        insert_duration.as_secs_f64()
    );

    // Allow time for any pending background operations
    println!("\n3. Allowing background operations to settle...");
    thread::sleep(Duration::from_millis(100));

    // Print final LSM tree state
    let mem = tree.mem.read().unwrap();
    println!("\nFinal LSM Tree State:");
    println!("------------------------");
    println!("Memory:");
    println!(
        "  Active size: {}",
        mem.active_size.load(std::sync::atomic::Ordering::Relaxed)
    );
    println!("  Frozen tables: {}", mem.frozen.len());
    println!("Disk levels:");
    println!(
        "  Level 0: {}",
        tree.disk.level_0.read().unwrap().sst_readers.len()
    );
    println!(
        "  Level 1: {}",
        tree.disk.level_1.read().unwrap().sst_readers.len()
    );
    println!(
        "  Level 2: {}",
        tree.disk.level_2.read().unwrap().sst_readers.len()
    );

    // Verify all keys have their latest version
    println!("\n4. Starting verification of latest versions...");
    let verify_start = Instant::now();
    let mut verified = 0;
    let mut errors = 0;
    let mut missing = 0;
    let mut mismatches = 0;

    for i in 0..n_keys {
        let key = format!("key{:05}", i);
        let expected_value = format!("value{:05}_v{}", i, n_versions - 1);
        let key_bytes = key.as_bytes();
        let expected_bytes = expected_value.as_bytes();

        match tree.get(key_bytes) {
            Some(actual_value) => {
                if actual_value == Bytes::copy_from_slice(expected_bytes) {
                    verified += 1;
                } else {
                    mismatches += 1;
                    errors += 1;
                    println!("Value mismatch for key: {}", key);
                    println!("  Expected: {}", expected_value);
                    println!("  Got: {:?}", String::from_utf8_lossy(&actual_value));
                }
            }
            None => {
                missing += 1;
                errors += 1;
                println!("Missing key: {}", key);
            }
        }

        if (verified + errors) % 1000 == 0 {
            println!(
                "Progress: {}/{} entries verified",
                verified + errors,
                n_keys
            );
        }
    }

    let verify_duration = verify_start.elapsed();

    println!("\nVerification Results:");
    println!("------------------------");
    println!("Total keys: {}", n_keys);
    println!("Successfully verified latest versions: {}", verified);
    println!("Missing entries: {}", missing);
    println!("Value mismatches: {}", mismatches);
    println!("Total errors: {}", errors);
    println!(
        "Verification time: {:.2} seconds",
        verify_duration.as_secs_f64()
    );
    println!(
        "Average verification rate: {:.2} ops/sec",
        n_keys as f64 / verify_duration.as_secs_f64()
    );
    println!(
        "Total test time: {:.2} seconds",
        start_time.elapsed().as_secs_f64()
    );

    assert_eq!(errors, 0, "Found {} errors in verification", errors);
    assert!(verified > 0, "No entries were verified");
    assert_eq!(verified, n_keys, "Not all entries were verified");

    println!("\n=== Serial Version Insert Test Completed Successfully ===");
}

#[test]
fn test_concurrent_overwrite_delete() {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::time::Instant;

    println!("\n=== Starting Concurrent Overwrite/Delete Test ===");

    // Create temporary directory for LSM tree
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_str().unwrap().to_string();

    println!("1. Configuring LSM Tree...");
    let config = LsmConfig {
        dir: dir_path.clone(),
        disk: DiskConfig {
            level_0_size_threshold: 65536,
            block_size_multiplier: 4,
            level_0_threshold: 16,
            level_1_threshold: 16,
            level_2_threshold: 16,
            // currently not used
            auto_compact: true,
        },
        memory: MemoryConfig {
            freeze_size: 1024,    // 1KB - smaller for more frequent freezes
            flush_size: 4 * 1024, // 4KB
        },
        sst: SstConfig {
            block_size: 4096,
            scale: 100,
            fpr: 0.01,
        },
    };

    let tree = Arc::new(LsmTree::empty(config));

    // Track the final expected state for each key
    let expected_state = Arc::new(RwLock::new(HashMap::new()));

    // Define test parameters - reduced for debugging
    let n_keys = 100; // Reduced number of unique keys
    let n_writers = 4; // Reduced number of writers
    let ops_per_writer = 10000; // Reduced operations per writer
    let total_ops = ops_per_writer * n_writers;

    println!(
        "2. Launching {} writer threads, {} ops each on {} unique keys...",
        n_writers, ops_per_writer, n_keys
    );

    // Spawn writer threads
    let mut write_handles = vec![];
    let start_time = Instant::now();

    for writer_id in 0..n_writers {
        let tree = Arc::clone(&tree);
        let expected_state = Arc::clone(&expected_state);

        let time = Instant::now();

        let handle = thread::spawn(move || {
            let mut rng = rand::rng();
            // println!("Writer {} starting", writer_id);

            for op_num in 0..ops_per_writer {
                if op_num % (ops_per_writer / 10) == 0 {
                    println!("Writer {} op {}", writer_id, op_num);
                }

                // Pick a random key from the fixed set
                let key_id = rng.random_range(0..n_keys);
                let key = format!("key{:05}", key_id).into_bytes();

                // 80% chance of put, 20% chance of delete
                if rng.random_ratio(8, 10) {
                    // Put operation with a unique value that identifies the writer and operation
                    let timestamp = time.elapsed().as_nanos();
                    let value =
                        format!("value{}-w{}-t[{}]", key_id, writer_id, timestamp).into_bytes();

                    // println!("Writer {} op {}: PUT key{:05}", writer_id, op_num, key_id);
                    tree.put(&key, &value);

                    // Update expected state
                    let mut state = expected_state.write().unwrap();
                    state.insert(key.clone(), value.clone());
                } else {
                    // Delete operation
                    // println!(
                    //     "Writer {} op {}: DELETE key{:05}",
                    //     writer_id, op_num, key_id
                    // );
                    tree.delete(&key);

                    // Update expected state
                    let mut state = expected_state.write().unwrap();
                    state.remove(&key);
                }

                // // Small sleep to allow interleaving
                // if rng.random_ratio(1, 10) {
                //     // Increased sleep frequency
                //     thread::sleep(Duration::from_micros(1000)); // Increased sleep duration
                // }
            }
            // println!("Writer {} completed", writer_id);
        });
        write_handles.push(handle);
    }

    // Wait for all writers to complete
    println!("3. Waiting for writers to complete...");
    for handle in write_handles {
        handle.join().unwrap();
    }
    let write_duration = start_time.elapsed();
    println!(
        "   All writers completed in {:.2} seconds",
        write_duration.as_secs_f64()
    );

    // Allow time for background operations to settle
    println!("4. Allowing background operations to settle...");
    thread::sleep(Duration::from_millis(500));

    println!("5. Starting verification...");
    let verify_start = Instant::now();

    // Print LSM Tree state
    let mem = tree.mem.read().unwrap();
    println!("\nLSM Tree State:");
    println!("------------------------");
    println!("Memory:");
    println!("  Active size: {}", mem.active_size.load(Ordering::Relaxed));
    println!("  Frozen tables: {}", mem.frozen.len());
    println!("Disk levels:");
    println!(
        "  Level 0: {}",
        tree.disk.level_0.read().unwrap().sst_readers.len()
    );
    println!(
        "  Level 1: {}",
        tree.disk.level_1.read().unwrap().sst_readers.len()
    );
    println!(
        "  Level 2: {}",
        tree.disk.level_2.read().unwrap().sst_readers.len()
    );

    // Verify all keys against expected state
    let expected = expected_state.read().unwrap();
    println!("\nData Verification:");
    println!("------------------------");
    println!("Total unique keys: {}", n_keys);
    println!("Keys in final state: {}", expected.len());

    let mut verified = 0;
    let mut errors = 0;

    // Verify every possible key
    for key_id in 0..n_keys {
        let key = format!("key{:05}", key_id).into_bytes();
        let tree_value = tree.get(&key);
        let expected_value = expected.get(&key);

        match (tree_value, expected_value) {
            (Some(actual), Some(expected)) => {
                if actual == Bytes::from(expected.clone()) {
                    verified += 1;
                } else {
                    errors += 1;
                    println!(
                        "Value mismatch for key: {:?}",
                        String::from_utf8_lossy(&key)
                    );
                    println!("  Expected: {:?}", String::from_utf8_lossy(expected));
                    println!("  Got: {:?}", String::from_utf8_lossy(&actual));
                }
            }
            (None, None) => {
                verified += 1; // Both agree key should not exist
            }
            (Some(actual), None) => {
                errors += 1;
                println!("Key{:05} exists but should be deleted:", key_id);
                println!("  Got: {:?}", String::from_utf8_lossy(&actual));
            }
            (None, Some(expected)) => {
                errors += 1;
                println!("Key{:05} missing but should exist:", key_id);
                println!("  Expected: {:?}", String::from_utf8_lossy(expected));
            }
        }
    }

    let verify_duration = verify_start.elapsed();

    println!("\nVerification Results:");
    println!("------------------------");
    println!("Successfully verified: {}/{}", verified, n_keys);
    println!("Errors: {}", errors);
    println!(
        "Verification time: {:.2} seconds",
        verify_duration.as_secs_f64()
    );
    println!(
        "Total test time: {:.2} seconds",
        start_time.elapsed().as_secs_f64()
    );

    assert_eq!(errors, 0, "Found {} errors in verification", errors);
    assert!(verified > 0, "No entries were verified");

    println!("\n=== Concurrent Overwrite/Delete Test Completed Successfully ===");
}

#[test]
fn test_close_repoen() {
    // let temp_dir = tempdir().unwrap();
    // let dir_path = temp_dir.path().to_str().unwrap().to_string();

    let dir_path = "./db".to_string();

    _ = fs::remove_dir_all(&dir_path);
    fs::create_dir(&dir_path).unwrap();

    let n_versions = 5;
    let n_keys = 10000;

    let keygen = |x| format!("KEY-{:010}-{:010}", x, 37474 - x);
    let valuegen = |x, rem| format!("VALUE-{:010}-{}", x, rem);

    {
        let config = LsmConfig {
            dir: dir_path.clone(),
            disk: DiskConfig {
                level_0_size_threshold: 4096,
                block_size_multiplier: 8,
                level_0_threshold: 16,
                level_1_threshold: 128,
                level_2_threshold: 1024,
                // currently not used
                auto_compact: false,
            },
            memory: MemoryConfig {
                freeze_size: 1024,    // 1KB - smaller for more frequent freezes
                flush_size: 4 * 1024, // 4KB
            },
            sst: SstConfig {
                block_size: 4096,
                scale: 100,
                fpr: 0.01,
            },
        };

        let tree = LsmTree::empty(config);

        for rem in 0..n_versions {
            for i in 0..n_keys {
                if i % n_versions == rem {
                    let key = keygen(i);

                    if rem == n_versions - 1 {
                        tree.delete(key.as_bytes());
                    } else {
                        let value = valuegen(i, rem);

                        tree.put(key.as_bytes(), value.as_bytes());
                    }
                }
            }
        }
    }

    println!("Insert completed");

    {
        let tree = LsmTree::load(dir_path);
        for i in 0..n_keys {
            let rem = i % n_versions;
            let key = keygen(i);

            if rem == n_versions - 1 {
                assert!(tree.get(key.as_bytes()).is_none());
            } else {
                let value = tree.get(key.as_bytes());
                // dbg!(&value);
                assert_eq!(value.unwrap(), valuegen(i, rem));
            }
        }

        ///////////////////////////// print stats /////////////////////////////
        let mem = tree.mem.read().unwrap();
        println!("\nLSM Tree State:");
        println!("------------------------");
        println!("Memory:");
        println!("  Active size: {}", mem.active_size.load(Ordering::Relaxed));
        println!("  Frozen tables: {}", mem.frozen.len());
        println!("Disk levels:");
        println!(
            "  Level 0: {}",
            tree.disk.level_0.read().unwrap().sst_readers.len()
        );
        println!(
            "  Level 1: {}",
            tree.disk.level_1.read().unwrap().sst_readers.len()
        );
        println!(
            "  Level 2: {}",
            tree.disk.level_2.read().unwrap().sst_readers.len()
        );
    }
}
