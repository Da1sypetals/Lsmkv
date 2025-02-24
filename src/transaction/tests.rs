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

    // ---------------------------- test logic ----------------------------
    let mut tx = tree.new_transaction();
    // put 3 values
    tree.put(b"k1", b"v1");
    tree.put(b"k2", b"v2");
    tree.put(b"k3", b"v3");

    // put 3 values via transaction
    tx.put(b"tx-k1", b"tx-v1");
    tx.put(b"tx-k2", b"tx-v2");
    tx.put(b"tx-k3", b"tx-v3");

    // verify the values are not visible by transaction
    assert_eq!(tx.get(b"k1"), None);
    assert_eq!(tx.get(b"k2"), None);
    assert_eq!(tx.get(b"k3"), None);

    // verify the values are visible by transaction
    assert_eq!(tx.get(b"tx-k1"), Some(Bytes::from("tx-v1")));
    assert_eq!(tx.get(b"tx-k2"), Some(Bytes::from("tx-v2")));
    assert_eq!(tx.get(b"tx-k3"), Some(Bytes::from("tx-v3")));

    // verify the transaction values are not visible by current get
    assert_eq!(tree.get(b"tx-k1"), None);
    assert_eq!(tree.get(b"tx-k2"), None);
    assert_eq!(tree.get(b"tx-k3"), None);

    // commit
    tx.commit();

    // verify the values are visible by current get
    assert_eq!(tree.get(b"tx-k1"), Some(Bytes::from("tx-v1")));
    assert_eq!(tree.get(b"tx-k2"), Some(Bytes::from("tx-v2")));
    assert_eq!(tree.get(b"tx-k3"), Some(Bytes::from("tx-v3")));

    // ---------------------------- test logic ----------------------------

    let mem = tree.mem.read().unwrap();
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
}

#[test]
fn test_bigger() {
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

    // ---------------------------- test logic ----------------------------
    let mut tx = tree.new_transaction();
    let entries = (0..10000)
        .map(|i| {
            (
                format!("k{}", i).into_bytes(),
                format!("v{}", i).into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    let tx_entries = (0..10000)
        .map(|i| {
            (
                format!("tx-k{}", i).into_bytes(),
                format!("tx-v{}", i).into_bytes(),
            )
        })
        .collect::<Vec<_>>();

    // put values
    for (key, value) in &entries {
        tree.put(key, value);
    }

    // put values via transaction
    for (key, value) in &tx_entries {
        tx.put(key, value);
    }

    // verify the values are not visible by transaction
    for (key, _) in &entries {
        assert_eq!(tx.get(key), None);
    }

    // verify the values are visible by transaction
    for (key, value) in &tx_entries {
        assert_eq!(tx.get(key), Some(Bytes::from(value.clone())));
    }

    // verify the transaction values are not visible by current get
    for (key, _) in &tx_entries {
        assert_eq!(tree.get(key), None);
    }

    // commit
    tx.commit();

    // verify the values are visible by current get
    for (key, value) in &tx_entries {
        assert_eq!(tree.get(key), Some(Bytes::from(value.clone())));
    }

    // ---------------------------- test logic ----------------------------

    let mem = tree.mem.read().unwrap();
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
}

#[test]
fn test_multiple_transactions() {
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

    // ---------------------------- test logic ----------------------------

    // Create two transactions
    let mut tx1 = tree.new_transaction();
    let mut tx2 = tree.new_transaction();

    // Transaction 1 writes
    tx1.put(b"tx1-k1", b"tx1-v1");
    tx1.put(b"shared-k", b"tx1-value");

    // Transaction 2 writes
    tx2.put(b"tx2-k1", b"tx2-v1");
    tx2.put(b"shared-k", b"tx2-value");

    // Direct tree writes
    tree.put(b"tree-k1", b"tree-v1");
    tree.put(b"shared-k", b"tree-value");

    // Verify isolation before any commits
    // 1. Transaction 1 should only see its own writes
    assert_eq!(tx1.get(b"tx1-k1"), Some(Bytes::from("tx1-v1")));
    assert_eq!(tx1.get(b"shared-k"), Some(Bytes::from("tx1-value")));
    assert_eq!(tx1.get(b"tx2-k1"), None);
    assert_eq!(tx1.get(b"tree-k1"), None);

    // 2. Transaction 2 should only see its own writes
    assert_eq!(tx2.get(b"tx2-k1"), Some(Bytes::from("tx2-v1")));
    assert_eq!(tx2.get(b"shared-k"), Some(Bytes::from("tx2-value")));
    assert_eq!(tx2.get(b"tx1-k1"), None);
    assert_eq!(tx2.get(b"tree-k1"), None);

    // 3. Tree should only see direct writes
    assert_eq!(tree.get(b"tree-k1"), Some(Bytes::from("tree-v1")));
    assert_eq!(tree.get(b"shared-k"), Some(Bytes::from("tree-value")));
    assert_eq!(tree.get(b"tx1-k1"), None);
    assert_eq!(tree.get(b"tx2-k1"), None);

    // Commit transaction 1
    tx1.commit();

    // Verify state after tx1 commit
    // 1. Tree should now see tx1's writes
    assert_eq!(tree.get(b"tx1-k1"), Some(Bytes::from("tx1-v1")));
    assert_eq!(tree.get(b"shared-k"), Some(Bytes::from("tx1-value")));
    assert_eq!(tree.get(b"tx2-k1"), None);

    // 2. Transaction 2 should still be isolated
    assert_eq!(tx2.get(b"tx2-k1"), Some(Bytes::from("tx2-v1")));
    assert_eq!(tx2.get(b"shared-k"), Some(Bytes::from("tx2-value")));
    assert_eq!(tx2.get(b"tx1-k1"), None);

    // Commit transaction 2
    tx2.commit();

    // Final state verification after all commits
    // 1. Tree should see tx2's writes (latest wins)
    assert_eq!(tree.get(b"tx1-k1"), Some(Bytes::from("tx1-v1")));
    assert_eq!(tree.get(b"tx2-k1"), Some(Bytes::from("tx2-v1")));
    assert_eq!(tree.get(b"shared-k"), Some(Bytes::from("tx2-value")));

    // ---------------------------- test logic ----------------------------

    let mem = tree.mem.read().unwrap();
    let report = format!(
        "Memory: active size: {}, # frozen tables: {}\nDisk: level_0: {}, level_1: {}, level_2: {}",
        mem.active_size.load(std::sync::atomic::Ordering::Relaxed),
        mem.frozen.len(),
        tree.disk.level_0.read().unwrap().sst_readers.len(),
        tree.disk.level_1.read().unwrap().sst_readers.len(),
        tree.disk.level_2.read().unwrap().sst_readers.len()
    );
    println!("{}", report);
}

#[test]
fn test_configurable_transactions() {
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

    // ---------------------------- test logic ----------------------------

    // Test configuration
    let num_transactions = 5;
    let keys_per_transaction = 100;
    let shared_keys = vec!["shared1", "shared2", "shared3"];

    // Create transactions and their entries
    let mut transactions = Vec::new();
    let mut transaction_entries = Vec::new();

    for tx_id in 0..num_transactions {
        let tx = tree.new_transaction();
        transactions.push(Some(tx));

        // Generate unique entries for this transaction
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..keys_per_transaction)
            .map(|i| {
                (
                    format!("tx{}-k{}", tx_id, i).into_bytes(),
                    format!("tx{}-v{}", tx_id, i).into_bytes(),
                )
            })
            .collect();
        transaction_entries.push(entries);
    }

    // Write data to transactions and verify isolation
    for (tx_id, (tx, entries)) in transactions
        .iter_mut()
        .zip(&transaction_entries)
        .enumerate()
    {
        let tx = tx.as_mut().unwrap();
        // Write unique entries
        for (key, value) in entries {
            tx.put(key, value);
        }

        // Write to shared keys
        for shared_key in &shared_keys {
            tx.put(
                shared_key.as_bytes(),
                format!("tx{}-shared-value", tx_id).as_bytes(),
            );
        }

        // Write some direct tree values
        tree.put(
            format!("direct-k{}", tx_id).as_bytes(),
            format!("direct-v{}", tx_id).as_bytes(),
        );
    }

    // Verify isolation before any commits
    for (tx_id, (tx, entries)) in transactions.iter().zip(&transaction_entries).enumerate() {
        let tx = tx.as_ref().unwrap();
        // 1. Transaction should see its own writes
        for (key, value) in entries {
            assert_eq!(tx.get(key), Some(Bytes::from(value.clone())));
        }

        // 2. Transaction should see its own shared key values
        for shared_key in &shared_keys {
            assert_eq!(
                tx.get(shared_key.as_bytes()),
                Some(Bytes::from(format!("tx{}-shared-value", tx_id)))
            );
        }

        // 3. Transaction should NOT see other transactions' writes
        for (other_tx_id, other_entries) in transaction_entries.iter().enumerate() {
            if other_tx_id != tx_id {
                for (key, _) in other_entries {
                    assert_eq!(tx.get(key), None);
                }
            }
        }

        // 4. Transaction should NOT see direct tree writes
        for i in 0..num_transactions {
            assert_eq!(tx.get(format!("direct-k{}", i).as_bytes()), None);
        }
    }

    // Commit transactions one by one and verify progressive visibility
    for commit_tx_id in 0..transactions.len() {
        // Commit this transaction
        if let Some(tx) = transactions[commit_tx_id].take() {
            tx.commit();
        }

        // Verify tree state after this commit
        // 1. Tree should see all committed transaction writes
        for (tx_id, entries) in transaction_entries.iter().enumerate() {
            if tx_id <= commit_tx_id {
                // Should see entries from committed transactions
                for (key, value) in entries {
                    assert_eq!(tree.get(key), Some(Bytes::from(value.clone())));
                }
            } else {
                // Should NOT see entries from uncommitted transactions
                for (key, _) in entries {
                    assert_eq!(tree.get(key), None);
                }
            }
        }

        // 2. Tree should see latest committed shared key values
        for shared_key in &shared_keys {
            assert_eq!(
                tree.get(shared_key.as_bytes()),
                Some(Bytes::from(format!("tx{}-shared-value", commit_tx_id)))
            );
        }

        // 3. Remaining uncommitted transactions should still be isolated
        for tx_id in (commit_tx_id + 1)..transactions.len() {
            if let Some(tx) = &transactions[tx_id] {
                let entries = &transaction_entries[tx_id];

                // Should still see own writes
                for (key, value) in entries {
                    assert_eq!(tx.get(key), Some(Bytes::from(value.clone())));
                }

                // Should NOT see other transactions' writes
                for (other_tx_id, other_entries) in transaction_entries.iter().enumerate() {
                    if other_tx_id != tx_id {
                        for (key, _) in other_entries {
                            assert_eq!(tx.get(key), None);
                        }
                    }
                }
            }
        }
    }

    // ---------------------------- test logic ----------------------------

    let mem = tree.mem.read().unwrap();
    let report = format!(
        "Memory: active size: {}, # frozen tables: {}\nDisk: level_0: {}, level_1: {}, level_2: {}",
        mem.active_size.load(std::sync::atomic::Ordering::Relaxed),
        mem.frozen.len(),
        tree.disk.level_0.read().unwrap().sst_readers.len(),
        tree.disk.level_1.read().unwrap().sst_readers.len(),
        tree.disk.level_2.read().unwrap().sst_readers.len()
    );
    println!("{}", report);
}

#[test]
fn test_transaction_durability() {
    // let temp_dir = tempdir().unwrap();
    // let dir_path = temp_dir.path().to_str().unwrap().to_string();

    let dir_path = "./db".to_string();

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

    // ---------------------------- test logic ----------------------------

    // First scenario: Transaction that fails to commit before shutdown
    {
        let mut tree = LsmTree::empty(config.clone());

        // Put some base data
        tree.put(b"base1", b"value1");
        tree.put(b"base2", b"value2");

        // Create a transaction but don't commit it
        let mut tx = tree.new_transaction();
        tx.put(b"tx-k1", b"tx-v1");
        tx.put(b"tx-k2", b"tx-v2");

        // Force persist current state
        tree.persist();

        // Simulate crash by dropping the tree
        drop(tree);

        // Reload the tree
        let tree = LsmTree::load(&dir_path);

        // Verify base data is there
        assert_eq!(tree.get(b"base1"), Some(Bytes::from("value1")));
        assert_eq!(tree.get(b"base2"), Some(Bytes::from("value2")));

        // Verify uncommitted transaction data is not there
        assert_eq!(tree.get(b"tx-k1"), None);
        assert_eq!(tree.get(b"tx-k2"), None);
    }
    println!("========== Scenario 1 OK =========");

    // Second scenario: Transaction that commits but might not be persisted
    {
        let mut tree = LsmTree::empty(config.clone());

        // Put some base data
        tree.put(b"base3", b"value3");
        tree.put(b"base4", b"value4");

        // Create and commit a transaction
        let mut tx = tree.new_transaction();
        tx.put(b"tx-k3", b"tx-v3");
        tx.put(b"tx-k4", b"tx-v4");
        tx.commit();

        // Don't explicitly persist, simulate immediate crash
        drop(tree);

        // Reload the tree
        let tree = LsmTree::load(&dir_path);

        // Verify base data is there
        assert_eq!(tree.get(b"base3"), Some(Bytes::from("value3")));
        assert_eq!(tree.get(b"base4"), Some(Bytes::from("value4")));

        // Verify committed transaction data is recovered from WAL
        assert_eq!(tree.get(b"tx-k3"), Some(Bytes::from("tx-v3")));
        assert_eq!(tree.get(b"tx-k4"), Some(Bytes::from("tx-v4")));
    }

    println!("========== Scenario 2 OK =========");

    // ---------------------------- test logic ----------------------------

    // Clean up is handled by tempdir
}

// todo here
