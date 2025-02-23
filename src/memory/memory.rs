use super::{memtable::Memtable, types::Record};
use crate::clock::Clock;
use bytes::Bytes;
use scc::{Queue, ebr::Guard};
use std::{
    collections::VecDeque,
    sync::{Arc, atomic::AtomicUsize},
};

pub struct LsmMemory {
    pub(crate) active: Arc<Memtable>,
    /// An approximation
    pub(crate) active_size: AtomicUsize,

    /// Newer ones have higher index.
    ///     - SST flush should not hold the write lock of LsmMemory
    ///     - thus vecdeque must support:
    ///         1. concurrency
    ///         2. .iter() (to support get operation)
    pub(crate) frozen: Queue<Arc<Memtable>>,
    pub(crate) frozen_sizes: Queue<usize>,
}

impl LsmMemory {
    pub fn empty() -> Self {
        Self {
            active: Arc::new(Memtable::new()),
            active_size: AtomicUsize::new(0),
            frozen: Queue::default(),
            frozen_sizes: Queue::default(),
        }
    }
}

impl LsmMemory {
    pub fn put(&self, key: &[u8], value: &[u8], timestamp: u64) {
        self.active_size.fetch_add(
            key.len() + value.len(),
            std::sync::atomic::Ordering::Relaxed,
        );
        self.active.put(key, value, timestamp);
    }

    /// - Current read
    /// - Fully managed my `Memtable::get` method
    pub fn get(&self, key: &[u8]) -> Option<Record> {
        let opt_rec_active = self.active.get(key);
        if opt_rec_active.is_some() {
            return opt_rec_active;
        }

        // Not found in active memtable

        let guard = Guard::new();

        for table in self.frozen.iter(&guard).collect::<Vec<_>>().iter().rev() {
            let frozen_value = table.get(key);
            if frozen_value.is_some() {
                return frozen_value;
            }
        }

        // ...And not found in any frozen memtable
        // Thus not found in the memory structure

        None
    }

    pub fn delete(&self, key: &[u8], timestamp: u64) {
        self.active.delete(key, timestamp);
    }

    /// Snapshot read
    pub fn get_at_time(&self, key: &[u8], timestamp: u64) -> Option<Record> {
        let opt_rec_active = self.active.get_at_time(key, timestamp);
        if opt_rec_active.is_some() {
            return opt_rec_active;
        }

        // Not found in active memtable

        let guard = Guard::new();

        for table in self.frozen.iter(&guard).collect::<Vec<_>>().iter().rev() {
            let frozen_value = table.get_at_time(key, timestamp);
            if frozen_value.is_some() {
                return frozen_value;
            }
        }

        // ...And not found in any frozen memtable
        // Thus not found in the memory structure

        None
    }
}

// privates
impl LsmMemory {
    pub(crate) fn try_freeze_current(&mut self, freeze_size: usize) {
        // println!("Freezing...");
        let current_size = self.active_size.load(std::sync::atomic::Ordering::Relaxed);
        if current_size >= freeze_size {
            // really freeze
            self.frozen.push(self.active.clone()); // clone the arc
            self.frozen_sizes.push(current_size);

            self.active = Arc::new(Memtable::new());
            self.active_size
                .store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub(crate) fn force_freeze_current(&mut self) {
        // println!("Freezing...");
        let current_size = self.active_size.load(std::sync::atomic::Ordering::Relaxed);
        // really freeze
        self.frozen.push(self.active.clone()); // clone the arc
        self.frozen_sizes.push(current_size);

        self.active = Arc::new(Memtable::new());
        self.active_size
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::sync::Arc;
//     use std::thread;

//     #[test]
//     fn test_multiple_memtables() {
//         let mut memory = LsmMemory {
//             active: Arc::new(Memtable::new()),
//             active_size: AtomicUsize::new(0),
//             frozen: Queue::default(),
//             frozen_sizes: Queue::default(),
//         };

//         // Insert data that should cause multiple freezes
//         for i in 0..1000 {
//             let key = format!("key{}", i).into_bytes();
//             let value = format!("value{}", i).into_bytes();
//             memory.put(&key, &value);

//             // Try to freeze after each insert with a relatively small freeze size
//             memory.try_freeze_current(1000);
//         }

//         // Verify we have multiple frozen memtables
//         assert!(memory.frozen.len() > 0, "Should have frozen memtables");
//         dbg!(memory.frozen.len());

//         // print each frozen memtable size
//         let guard = Guard::new();
//         for table in memory.frozen.iter(&guard) {
//             let size: usize = table
//                 .map
//                 .iter()
//                 // if as search result is none, return 0
//                 .map(|kv| {
//                     kv.key().len()
//                         + if let Some(rec) = kv.value().clone().as_search_result() {
//                             rec.len()
//                         } else {
//                             0
//                         }
//                 })
//                 .sum();
//             println!("Frozen memtable size: {}", size);
//         }

//         // Verify we can still read data from both active and frozen memtables
//         for i in 0..1000 {
//             let key = format!("key{}", i).into_bytes();
//             let value = memory.get(&key);
//             assert!(value.is_some(), "Failed to retrieve key{}", i);
//             assert_eq!(
//                 value.unwrap(),
//                 Record::Value(format!("value{}", i).into()),
//                 "Incorrect value for key{}",
//                 i
//             );
//         }

//         // Test deletion
//         let test_key = b"key0";
//         memory.delete(test_key);
//         assert!(
//             matches!(memory.get(test_key).unwrap(), Record::Tomb),
//             "Key should be deleted"
//         );

//         dbg!(memory.frozen.len());
//     }

//     #[test]
//     fn test_concurrent_access() {
//         let memory = Arc::new(LsmMemory {
//             active: Arc::new(Memtable::new()),
//             active_size: AtomicUsize::new(0),
//             frozen: Queue::default(),
//             frozen_sizes: Queue::default(),
//         });
//         let mut handles = vec![];

//         // Spawn multiple threads to perform operations
//         for i in 0..50 {
//             let mem = memory.clone();
//             let handle = thread::spawn(move || {
//                 for j in 0..100 {
//                     let key = format!("key{}_{}", i, j).into_bytes();
//                     let value = format!("value{}_{}", i, j).into_bytes();
//                     mem.put(&key, &value);

//                     // Verify the write
//                     let result = mem.get(&key);
//                     assert_eq!(
//                         result.unwrap(),
//                         Record::Value(Bytes::copy_from_slice(&value))
//                     );
//                 }
//             });
//             handles.push(handle);
//         }

//         // Wait for all threads
//         for handle in handles {
//             handle.join().unwrap();
//         }
//     }

//     #[test]
//     fn test_freeze_under_load() {
//         let mut memory = LsmMemory {
//             active: Arc::new(Memtable::new()),
//             active_size: AtomicUsize::new(0),
//             frozen: Queue::default(),
//             frozen_sizes: Queue::default(),
//         };

//         // Insert data with varying sizes to trigger multiple freezes
//         for i in 0..100 {
//             let key = format!("key{}", i).into_bytes();
//             // Create values of increasing size
//             let value = vec![b'x'; i * 100];
//             memory.put(&key, &value);
//             memory.try_freeze_current(1000); // Small freeze size to trigger frequent freezes
//         }

//         // Verify frozen memtable count
//         assert!(
//             memory.frozen.len() > 2,
//             "Should have multiple frozen memtables"
//         );

//         // print each frozen memtable size
//         let guard = Guard::new();
//         for table in memory.frozen.iter(&guard) {
//             let size: usize = table
//                 .map
//                 .iter()
//                 // if as search result is none, return 0
//                 .map(|kv| {
//                     kv.key().len()
//                         + if let Some(rec) = kv.value().clone().as_search_result() {
//                             rec.len()
//                         } else {
//                             0
//                         }
//                 })
//                 .sum();
//             println!("Frozen memtable size: {}", size);
//         }

//         // Verify data is still accessible
//         for i in 0..100 {
//             let key = format!("key{}", i).into_bytes();
//             let value = vec![b'x'; i * 100];
//             let result = memory.get(&key);
//             assert_eq!(result.unwrap(), Record::Value(Bytes::from(value)));
//         }
//     }

//     #[test]
//     fn test_mixed_operations() {
//         let mut memory = LsmMemory {
//             active: Arc::new(Memtable::new()),
//             active_size: AtomicUsize::new(0),
//             frozen: Queue::default(),
//             frozen_sizes: Queue::default(),
//         };

//         // Mix of puts, gets, and deletes
//         for i in 0..100 {
//             let key = format!("key{}", i).into_bytes();
//             let value = format!("value{}", i).into_bytes();

//             // Put
//             memory.put(&key, &value);

//             // Get (should exist)
//             assert_eq!(
//                 memory.get(&key).unwrap(),
//                 Record::Value(Bytes::from(value.clone()))
//             );

//             // Delete every third key
//             if i % 3 == 0 {
//                 memory.delete(&key);
//                 assert!(matches!(memory.get(&key).unwrap(), Record::Tomb));
//             }

//             memory.try_freeze_current(1000);
//         }

//         // Verify final state
//         for i in 0..100 {
//             let key = format!("key{}", i).into_bytes();
//             let value = format!("value{}", i).into_bytes();

//             if i % 3 == 0 {
//                 // Deleted keys should not exist
//                 assert!(matches!(memory.get(&key).unwrap(), Record::Tomb));
//             } else {
//                 // Other keys should exist with correct values
//                 assert_eq!(memory.get(&key).unwrap(), Record::Value(Bytes::from(value)));
//             }
//         }
//     }

//     #[test]
//     fn test_edge_cases() {
//         let mut memory = LsmMemory {
//             active: Arc::new(Memtable::new()),
//             active_size: AtomicUsize::new(0),
//             frozen: Queue::default(),
//             frozen_sizes: Queue::default(),
//         };

//         // Test empty key/value
//         memory.put(b"", b"empty_key");
//         memory.put(b"empty_value", b"");
//         memory.put(b"", b"");

//         assert_eq!(memory.get(b"").unwrap(), Record::Value(Bytes::from("")));
//         assert_eq!(
//             memory.get(b"empty_value").unwrap(),
//             Record::Value(Bytes::from(""))
//         );

//         // Test large values
//         let large_value = vec![b'x'; 1_000_000];
//         memory.put(b"large_key", &large_value);
//         assert_eq!(
//             memory.get(b"large_key").unwrap(),
//             Record::Value(Bytes::from(large_value))
//         );

//         // Test delete after freeze
//         memory.try_freeze_current(100);
//         memory.delete(b"large_key");
//         assert!(matches!(memory.get(b"large_key").unwrap(), Record::Tomb));
//     }

//     #[test]
//     fn test_high_pressure_concurrent_access() {
//         let memory = Arc::new(LsmMemory {
//             active: Arc::new(Memtable::new()),
//             active_size: AtomicUsize::new(0),
//             frozen: Queue::default(),
//             frozen_sizes: Queue::default(),
//         });

//         const NUM_WRITERS: usize = 20;
//         const NUM_READERS: usize = 30;
//         const OPS_PER_THREAD: usize = 1000;
//         let mut handles = vec![];

//         // Spawn writer threads
//         for i in 0..NUM_WRITERS {
//             let mem = memory.clone();
//             let handle = thread::spawn(move || {
//                 for j in 0..OPS_PER_THREAD {
//                     // Use modulo to create key contention between threads
//                     let key_id = (i * j) % 100;
//                     let key = format!("key{}", key_id).into_bytes();
//                     let value = format!("value{}-{}", key_id, j).into_bytes();
//                     mem.put(&key, &value);

//                     // Occasional read of own writes
//                     // if j % 10 == 0 {
//                     //     dbg!(mem.get(&key));
//                     //     let result = mem.get(&key);
//                     //     // if result.is_none() {
//                     //     //     dbg!(&result);
//                     //     //     dbg!(mem.get(&key));
//                     //     // }
//                     //     assert!(
//                     //         result.is_some(),
//                     //         "Failed to read own write of {}",
//                     //         String::from_utf8_lossy(&key)
//                     //     );
//                     // }
//                 }
//             });
//             handles.push(handle);
//         }

//         // Spawn reader threads that continuously read
//         for _ in 0..NUM_READERS {
//             let mem = memory.clone();
//             let handle = thread::spawn(move || {
//                 for _ in 0..OPS_PER_THREAD {
//                     // Read random keys from the possible key space
//                     let key_id = rand::random_range(0..100);
//                     let key = format!("key{}", key_id).into_bytes();
//                     // Reads might return None which is fine - just shouldn't panic
//                     let _result = mem.get(&key);
//                 }
//             });
//             handles.push(handle);
//         }

//         // Wait for all threads
//         for handle in handles {
//             handle.join().unwrap();
//         }

//         // Verify we can still read all keys
//         for i in 0..100 {
//             let key = format!("key{}", i).into_bytes();
//             let result = memory.get(&key);
//             // Key should either be None or contain a valid value
//             if let Some(value) = result {
//                 match value {
//                     Record::Value(bytes) => {
//                         assert!(
//                             String::from_utf8_lossy(&bytes).starts_with(&format!("value{}-", i))
//                         );
//                     }
//                     _ => panic!("Expected a value record"),
//                 }
//             }
//         }
//     }

//     #[test]
//     fn test_serial_read_after_write() {
//         let mut memory = LsmMemory {
//             active: Arc::new(Memtable::new()),
//             active_size: AtomicUsize::new(0),
//             frozen: Queue::default(),
//             frozen_sizes: Queue::default(),
//         };

//         // Test without freezing
//         for i in 0..100 {
//             let key = format!("key{}", i).into_bytes();
//             let value = format!("value{}", i).into_bytes();

//             // Write
//             memory.put(&key, &value);

//             // Immediate read
//             let result = memory.get(&key);
//             assert!(
//                 result.is_some(),
//                 "Failed to read key {} immediately after write",
//                 i
//             );
//             assert_eq!(
//                 result.unwrap(),
//                 Record::Value(Bytes::from(value.clone())),
//                 "Wrong value read for key {}",
//                 i
//             );
//         }

//         // Test with freezing after each write
//         for i in 100..200 {
//             let key = format!("key{}", i).into_bytes();
//             let value = format!("value{}", i).into_bytes();

//             // Write
//             memory.put(&key, &value);

//             // Try to freeze
//             memory.try_freeze_current(100); // Small size to encourage freezing

//             // Read after potential freeze
//             let result = memory.get(&key);
//             assert!(result.is_some(), "Failed to read key {} after freeze", i);
//             assert_eq!(
//                 result.unwrap(),
//                 Record::Value(Bytes::from(value.clone())),
//                 "Wrong value read for key {} after freeze",
//                 i
//             );
//         }

//         // Verify all writes are still readable
//         for i in 0..200 {
//             let key = format!("key{}", i).into_bytes();
//             let expected_value = format!("value{}", i).into_bytes();
//             let result = memory.get(&key);

//             assert!(
//                 result.is_some(),
//                 "Failed to read key {} in final verification",
//                 i
//             );
//             assert_eq!(
//                 result.unwrap(),
//                 Record::Value(Bytes::from(expected_value)),
//                 "Wrong value read for key {} in final verification",
//                 i
//             );
//         }

//         // Print memtable sizes for debugging
//         println!(
//             "Active memtable size: {}",
//             memory
//                 .active_size
//                 .load(std::sync::atomic::Ordering::Relaxed)
//         );
//         println!("Number of frozen memtables: {}", memory.frozen.len());
//         let guard = Guard::new();
//         for (i, table) in memory.frozen.iter(&guard).enumerate() {
//             let size: usize = table
//                 .map
//                 .iter()
//                 .map(|kv| {
//                     kv.key().len()
//                         + if let Some(rec) = kv.value().clone().as_search_result() {
//                             rec.len()
//                         } else {
//                             0
//                         }
//                 })
//                 .sum();
//             println!("Frozen memtable {} size: {}", i, size);
//         }
//     }
// }

// #[cfg(test)]
// mod test_queue_iter_direction {
//     use super::*;

//     #[test]
//     fn test_queue_iter_direction() {
//         let queue = Queue::default();
//         queue.push(1);
//         queue.push(2);
//         queue.push(3);

//         let guard = Guard::new();
//         for i in queue.iter(&guard) {
//             print!("{}", i);
//         }

//         println!();

//         queue.pop();

//         let guard = Guard::new();
//         for i in queue.iter(&guard) {
//             print!("{}", i);
//         }
//         println!();
//     }
// }
