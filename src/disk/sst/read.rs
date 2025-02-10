use crate::disk::sst::Index;
use crate::memory::record::Record;
use bytes::Bytes;
use std::fs::File;
use std::io::Read;
use std::io::Seek;

#[derive(Debug, PartialEq, Clone, Copy)]
/// This is index in the Index array.
pub enum BisectResult {
    //
    //                 Exact(idx)
    //                 |
    //                 v
    // +--------------+--------------+--------------+
    // | key, offset  | key, offset  | key, offset  |
    // +--------------+--------------+--------------+
    //
    /// Use: index[idx] to get the exact offset
    Exact(usize),
    //
    //                 left                          right
    //                 |                             |
    //                 v                             v
    // +--------------+--------------+--------------+--------------+
    // | key, offset  | key, offset  | key, offset  | key, offset  |
    // +--------------+--------------+--------------+--------------+
    //
    /// Use: index[left], index[right] to get the range of two offsets
    Range(usize, usize),
}

pub struct SstReader {
    dir: String,
    filename: String,
}

impl SstReader {
    /// Returns:
    ///  - None if the key is not found
    ///  - Some(Value) if the key is found
    ///  - Some(Tomb) if the key is a tombstone
    pub fn get(&self, key: &[u8]) -> Option<Record> {
        let index = self.get_index();

        // dbg!(&index.len());

        match self.bisect_index(&index, key) {
            Some(BisectResult::Range(left, right)) => {
                let left_offset = index[left].1;
                let right_offset = index[right].1;
                // println!("Left: {}, Right: {}", left, right);
                let mut file = File::open(format!("{}/{}.data", self.dir, self.filename)).unwrap();

                // Seek to the left offset position
                file.seek(std::io::SeekFrom::Start(left_offset as u64))
                    .unwrap();

                // Read and decode records until we find the key or reach right offset
                let mut pos = left_offset as u64;
                let mut buf = Vec::new();

                while pos <= right_offset {
                    // Read record type
                    buf.resize(1, 0);
                    file.read_exact(&mut buf).unwrap();
                    let record_type = buf[0];

                    match record_type {
                        /*
                        If record is value:
                            if key match, retrieve value;
                            else, seek to next kv pair.
                        */
                        0 => {
                            // Value record
                            // Read key size
                            buf.resize(2, 0);
                            file.read_exact(&mut buf).unwrap();
                            let key_size =
                                u16::from_le_bytes(buf.to_vec().try_into().unwrap()) as usize;

                            // Read value size
                            buf.resize(2, 0);
                            file.read_exact(&mut buf).unwrap();
                            let value_size =
                                u16::from_le_bytes(buf.to_vec().try_into().unwrap()) as usize;

                            // Read key
                            buf.resize(key_size, 0);
                            file.read_exact(&mut buf).unwrap();

                            // Check if this is our key
                            if &buf == key {
                                // Read value
                                buf.resize(value_size, 0);
                                file.read_exact(&mut buf).unwrap();

                                return Some(Record::Value(Bytes::copy_from_slice(&buf)));
                            } else {
                                file.seek(std::io::SeekFrom::Current(value_size as i64))
                                    .unwrap();
                            }
                        }
                        1 => {
                            // Tomb record
                            // Read key size
                            buf.resize(2, 0);
                            file.read_exact(&mut buf).unwrap();
                            let key_size =
                                u16::from_le_bytes(buf.to_vec().try_into().unwrap()) as usize;

                            // Read key
                            buf.resize(key_size, 0);
                            file.read_exact(&mut buf).unwrap();

                            if &buf == key {
                                // Read tomb
                                return Some(Record::Tomb);
                            }
                            // else do nothing (to skip this value), since cursor is at the end of this KV pair.
                        }
                        type_id => panic!("Invalid record type: type_id = {}", type_id),
                    }

                    pos = file.stream_position().unwrap();
                    // println!("Pos: {}", pos);
                }

                None
            }
            Some(BisectResult::Exact(idx)) => {
                let offset = index[idx as usize].1;
                let mut file = File::open(format!("{}/{}.data", self.dir, self.filename)).unwrap();
                file.seek(std::io::SeekFrom::Start(offset as u64)).unwrap();

                let mut buf = Vec::new();

                // Read record type
                buf.resize(1, 0);
                file.read_exact(&mut buf).unwrap();
                let record_type = buf[0];

                match record_type {
                    0 => {
                        // Value record
                        // Read key size
                        buf.resize(2, 0);
                        file.read_exact(&mut buf).unwrap();
                        let key_size =
                            u16::from_le_bytes(buf.to_vec().try_into().unwrap()) as usize;

                        // Read value size
                        buf.resize(2, 0);
                        file.read_exact(&mut buf).unwrap();
                        let value_size =
                            u16::from_le_bytes(buf.to_vec().try_into().unwrap()) as usize;

                        // Read key
                        buf.resize(key_size, 0);
                        file.read_exact(&mut buf).unwrap();

                        // Verify this is our key (it should be, since we got an exact match)
                        // also  run on release mode
                        assert_eq!(&buf, key, "Key mismatch in exact match case");

                        // Read value
                        buf.resize(value_size, 0);
                        file.read_exact(&mut buf).unwrap();

                        Some(Record::Value(Bytes::copy_from_slice(&buf)))
                    }
                    1 => {
                        // Tomb record
                        buf.resize(2, 0);
                        file.read_exact(&mut buf).unwrap();
                        let key_size =
                            u16::from_le_bytes(buf.to_vec().try_into().unwrap()) as usize;

                        // Read key
                        buf.resize(key_size, 0);
                        file.read_exact(&mut buf).unwrap();

                        // Verify this is our key (it should be, since we got an exact match)
                        // also  run on release mode
                        assert_eq!(&buf, key, "Key mismatch in exact match case");

                        Some(Record::Tomb)
                    }
                    type_id => panic!("Invalid record type: type_id = {}", type_id),
                }
            }
            None => {
                // println!("Not found!");
                None
            }
        }
    }

    /// Bisect the key in the index, returning the nearest index in Index array
    /// Returns:
    ///  - None if the key is not found
    ///  - Some(BisectResult::Exact(idx)) if the key is found exactly
    ///  - Some(BisectResult::Range(left, right)) if the key is not found but in the range of two keys in Index array
    fn bisect_index(&self, index: &Index, key: &[u8]) -> Option<BisectResult> {
        if index.is_empty() {
            return None;
        }

        let mut left = 0;
        let mut right = index.len() - 1;

        // If key is outside the range of all keys
        if key < &index[0].0 || key > &index[right].0 {
            return None;
        }

        // Binary search
        while left <= right {
            let mid = left + (right - left) / 2;
            match key.cmp(&index[mid].0) {
                std::cmp::Ordering::Equal => return Some(BisectResult::Exact(mid)),
                std::cmp::Ordering::Less => {
                    if mid == 0 {
                        return None;
                    }
                    right = mid - 1;
                }
                std::cmp::Ordering::Greater => {
                    if mid == index.len() - 1 {
                        return None;
                    }
                    left = mid + 1;
                }
            }
        }

        // If we get here, the key wasn't found exactly
        // left is now the insertion point
        // right is the largest element smaller than key
        Some(BisectResult::Range(right, left))
    }

    fn get_index(&self) -> Index {
        let mut index = Index::new();
        let mut file = File::open(format!("{}/{}.index", self.dir, self.filename)).unwrap();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).unwrap();

        let mut pos = 0;
        while pos < contents.len() {
            // Read key length (u16)
            let key_len = u16::from_le_bytes(contents[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;

            // Read key
            let key = Bytes::copy_from_slice(&contents[pos..pos + key_len]);
            pos += key_len;

            // Read offset (u64)
            let offset = u64::from_le_bytes(contents[pos..pos + 8].try_into().unwrap());
            pos += 8;

            index.push((key, offset));
        }

        index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::sst::SstConfig;
    use crate::disk::sst::write::SstWriter;
    use crate::memory::memtable::Memtable;
    use crossbeam_skiplist::SkipMap;
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;
    use tempfile::tempdir;
    use tempfile::NamedTempFile;

    fn create_test_index_file(dir: &str, filename: &str, entries: &[(Vec<u8>, u64)]) {
        let mut temp_file = NamedTempFile::new_in(dir).unwrap();
        let file_path = temp_file.path().to_str().unwrap().to_string();

        for (key, offset) in entries {
            let key_len = key.len() as u16;
            temp_file.write_all(&key_len.to_le_bytes()).unwrap();
            temp_file.write_all(key).unwrap();
            temp_file.write_all(&offset.to_le_bytes()).unwrap();
        }
        temp_file.flush().unwrap();

        // Move the temporary file to the desired location
        let final_path = format!("{}/{}.index", dir, filename);
        temp_file.persist(final_path).unwrap();
    }

    fn create_test_data_file(dir: &str, filename: &str, records: &[(Vec<u8>, Option<Vec<u8>>)]) {
        let mut temp_file = NamedTempFile::new_in(dir).unwrap();

        for (key, value_opt) in records {
            match value_opt {
                Some(value) => {
                    // Write Value record
                    temp_file.write_all(&[0u8]).unwrap(); // record type
                    temp_file
                        .write_all(&(key.len() as u16).to_le_bytes())
                        .unwrap(); // key size
                    temp_file.write_all(key).unwrap(); // key
                    temp_file
                        .write_all(&(value.len() as u16).to_le_bytes())
                        .unwrap(); // value size
                    temp_file.write_all(value).unwrap(); // value
                }
                None => {
                    // Write Tomb record
                    temp_file.write_all(&[1u8]).unwrap(); // record type
                }
            }
        }
        temp_file.flush().unwrap();

        // Move the temporary file to the desired location
        let final_path = format!("{}/{}.data", dir, filename);
        temp_file.persist(final_path).unwrap();
    }

    #[test]
    fn test_get_index_empty() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();

        create_test_index_file(&dir_path, "test_empty", &[]);

        let reader = SstReader {
            dir: dir_path,
            filename: "test_empty".to_string(),
        };

        let index = reader.get_index();
        assert!(index.is_empty());
    }

    #[test]
    fn test_get_index_single_entry() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();

        let entries = vec![(b"key1".to_vec(), 42u64)];
        create_test_index_file(&dir_path, "test_single", &entries);

        let reader = SstReader {
            dir: dir_path,
            filename: "test_single".to_string(),
        };

        let index = reader.get_index();
        assert_eq!(index.len(), 1);
        assert_eq!(index[0].0, Bytes::from("key1"));
        assert_eq!(index[0].1, 42);
    }

    #[test]
    fn test_get_index_multiple_entries() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();

        let entries = vec![
            (b"key1".to_vec(), 42u64),
            (b"key2".to_vec(), 100u64),
            (b"key3".to_vec(), 200u64),
        ];
        create_test_index_file(&dir_path, "test_multiple", &entries);

        let reader = SstReader {
            dir: dir_path,
            filename: "test_multiple".to_string(),
        };

        let index = reader.get_index();
        assert_eq!(index.len(), 3);

        assert_eq!(index[0].0, Bytes::from("key1"));
        assert_eq!(index[0].1, 42);

        assert_eq!(index[1].0, Bytes::from("key2"));
        assert_eq!(index[1].1, 100);

        assert_eq!(index[2].0, Bytes::from("key3"));
        assert_eq!(index[2].1, 200);
    }

    #[test]
    fn test_get_index_varying_key_lengths() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();

        let entries = vec![
            (b"k".to_vec(), 1u64),
            (b"key".to_vec(), 2u64),
            (b"very_long_key_name".to_vec(), 3u64),
        ];
        create_test_index_file(&dir_path, "test_lengths", &entries);

        let reader = SstReader {
            dir: dir_path,
            filename: "test_lengths".to_string(),
        };

        let index = reader.get_index();
        assert_eq!(index.len(), 3);

        assert_eq!(index[0].0, Bytes::from("k"));
        assert_eq!(index[0].1, 1);

        assert_eq!(index[1].0, Bytes::from("key"));
        assert_eq!(index[1].1, 2);

        assert_eq!(index[2].0, Bytes::from("very_long_key_name"));
        assert_eq!(index[2].1, 3);
    }

    #[test]
    fn test_bisect_index_empty() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();
        let reader = SstReader {
            dir: dir_path,
            filename: "test".to_string(),
        };

        let index = Vec::new();
        assert_eq!(reader.bisect_index(&index, b"any_key"), None);
    }

    #[test]
    fn test_bisect_index_exact_match() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();
        let reader = SstReader {
            dir: dir_path,
            filename: "test".to_string(),
        };

        let index = vec![
            (Bytes::from("key1"), 10),
            (Bytes::from("key2"), 20),
            (Bytes::from("key3"), 30),
        ];

        // Test exact matches
        assert_eq!(
            reader.bisect_index(&index, b"key1"),
            Some(BisectResult::Exact(0))
        );
        assert_eq!(
            reader.bisect_index(&index, b"key2"),
            Some(BisectResult::Exact(1))
        );
        assert_eq!(
            reader.bisect_index(&index, b"key3"),
            Some(BisectResult::Exact(2))
        );
    }

    #[test]
    fn test_bisect_index_between_keys() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();
        let reader = SstReader {
            dir: dir_path,
            filename: "test".to_string(),
        };

        let index = vec![
            (Bytes::from("key1"), 10),
            (Bytes::from("key3"), 20),
            (Bytes::from("key5"), 30),
        ];

        // Test keys that fall between existing keys
        assert_eq!(
            reader.bisect_index(&index, b"key2"),
            Some(BisectResult::Range(0, 1))
        );
        assert_eq!(
            reader.bisect_index(&index, b"key4"),
            Some(BisectResult::Range(1, 2))
        );
    }

    #[test]
    fn test_bisect_index_out_of_range() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();
        let reader = SstReader {
            dir: dir_path,
            filename: "test".to_string(),
        };

        let index = vec![
            (Bytes::from("key1"), 10),
            (Bytes::from("key2"), 20),
            (Bytes::from("key3"), 30),
        ];

        // Test keys outside the range
        assert_eq!(reader.bisect_index(&index, b"key0"), None); // Before first key
        assert_eq!(reader.bisect_index(&index, b"key4"), None); // After last key
    }

    #[test]
    fn test_bisect_index_single_element() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();
        let reader = SstReader {
            dir: dir_path,
            filename: "test".to_string(),
        };

        let index = vec![(Bytes::from("key1"), 10)];

        // Test with a single element
        assert_eq!(reader.bisect_index(&index, b"key0"), None); // Before the key
        assert_eq!(
            reader.bisect_index(&index, b"key1"),
            Some(BisectResult::Exact(0))
        ); // Exact match
        assert_eq!(reader.bisect_index(&index, b"key2"), None); // After the key
    }

    fn create_sequential_entries(count: usize) -> Vec<(Vec<u8>, u64)> {
        (0..count)
            .map(|i| {
                let key = format!("key{:010}", i).into_bytes();
                let offset = (i * 100) as u64; // Simulate realistic offsets
                (key, offset)
            })
            .collect()
    }

    fn create_sparse_entries(count: usize, gap: usize) -> Vec<(Vec<u8>, u64)> {
        (0..count)
            .map(|i| {
                let key = format!("key{:010}", i * gap).into_bytes();
                let offset = (i * 100) as u64;
                (key, offset)
            })
            .collect()
    }

    #[test]
    fn test_large_sequential_index() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();

        // Create 1000 sequential entries
        let entries = create_sequential_entries(1000);
        create_test_index_file(&dir_path, "test_large_seq", &entries);

        let reader = SstReader {
            dir: dir_path,
            filename: "test_large_seq".to_string(),
        };

        let index = reader.get_index();
        assert_eq!(index.len(), 1000);

        // Test exact matches at different positions
        assert_eq!(
            reader.bisect_index(&index, b"key0000000000"),
            Some(BisectResult::Exact(0))
        );
        assert_eq!(
            reader.bisect_index(&index, b"key0000000500"),
            Some(BisectResult::Exact(500))
        );
        assert_eq!(
            reader.bisect_index(&index, b"key0000000999"),
            Some(BisectResult::Exact(999))
        );

        // Test values between entries
        assert_eq!(
            reader.bisect_index(&index, b"key0000000123.5"),
            Some(BisectResult::Range(123, 124))
        );

        // Test out of range values
        assert_eq!(reader.bisect_index(&index, b"key0000001000"), None);
        assert_eq!(reader.bisect_index(&index, b"aaa"), None);
        assert_eq!(reader.bisect_index(&index, b"zzz"), None);
    }

    #[test]
    fn test_large_sparse_index() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();

        // Create 100 entries with gaps of 100 between keys
        let entries = create_sparse_entries(100, 100);
        create_test_index_file(&dir_path, "test_large_sparse", &entries);

        let reader = SstReader {
            dir: dir_path,
            filename: "test_large_sparse".to_string(),
        };

        let index = reader.get_index();
        assert_eq!(index.len(), 100);

        // Test exact matches
        assert_eq!(
            reader.bisect_index(&index, b"key0000000000"),
            Some(BisectResult::Exact(0))
        );
        assert_eq!(
            reader.bisect_index(&index, b"key0000005000"),
            Some(BisectResult::Exact(50))
        );
        assert_eq!(
            reader.bisect_index(&index, b"key0000009900"),
            Some(BisectResult::Exact(99))
        );

        // Test values between sparse entries
        assert_eq!(
            reader.bisect_index(&index, b"key0000000050"),
            Some(BisectResult::Range(0, 1))
        );
        assert_eq!(
            reader.bisect_index(&index, b"key0000005050"),
            Some(BisectResult::Range(50, 51))
        );
    }

    #[test]
    fn test_index_binary_search_efficiency() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();

        // Create 10000 entries to test binary search efficiency
        let entries = create_sequential_entries(10000);
        create_test_index_file(&dir_path, "test_binary_search", &entries);

        let reader = SstReader {
            dir: dir_path,
            filename: "test_binary_search".to_string(),
        };

        let index = reader.get_index();

        // Test binary search with various patterns
        let test_patterns = [
            "key0000000000", // Start
            "key0000002500", // Quarter
            "key0000005000", // Middle
            "key0000007500", // Three quarters
            "key0000009999", // End
        ];

        for &pattern in &test_patterns {
            let result = reader.bisect_index(&index, pattern.as_bytes());
            let expected_idx = pattern[3..13].parse::<usize>().unwrap();
            assert_eq!(result, Some(BisectResult::Exact(expected_idx)));
        }

        // Test binary search with values between entries
        let between_patterns = [
            ("key0000000000.5", BisectResult::Range(0, 1)),
            ("key0000002500.5", BisectResult::Range(2500, 2501)),
            ("key0000005000.5", BisectResult::Range(5000, 5001)),
            ("key0000007500.5", BisectResult::Range(7500, 7501)),
            ("key0000009998.5", BisectResult::Range(9998, 9999)),
        ];

        for (pattern, expected) in &between_patterns {
            assert_eq!(
                reader.bisect_index(&index, pattern.as_bytes()),
                Some(*expected)
            );
        }
    }

    #[test]
    fn test_get_basic() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();
        let test_name = "test_basic";

        // Create test data in memtable
        let memtable = Arc::new(Memtable::new());
        memtable.put(b"key1", b"value1");
        memtable.put(b"key2", b"value2");
        memtable.delete(b"key3"); // Add a tombstone

        // Write SST file
        let config = SstConfig { block_size: 4096 };
        let writer = SstWriter::new(
            config,
            dir_path.clone(),
            test_name.to_string(),
            memtable.clone(),
        );
        writer.build();

        // Read using SstReader
        let reader = SstReader {
            dir: dir_path,
            filename: test_name.to_string(),
        };

        // Test getting existing values
        match reader.get(b"key1") {
            Some(Record::Value(value)) => assert_eq!(&value[..], b"value1"),
            _ => panic!("Expected value record for key1"),
        }

        match reader.get(b"key2") {
            Some(Record::Value(value)) => assert_eq!(&value[..], b"value2"),
            _ => panic!("Expected value record for key2"),
        }

        // Test getting tombstone
        match reader.get(b"key3") {
            Some(Record::Tomb) => (), // Expected
            _ => panic!("Expected tomb record for key3"),
        }

        // Test getting non-existent key
        assert!(reader.get(b"key4").is_none());
    }

    #[test]
    fn test_get_with_block_boundaries() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();
        let test_name = "test_blocks";

        // Create test data in memtable with large values to force block boundaries
        let memtable = Arc::new(Memtable::new());
        let large_value: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();

        for i in 0..10 {
            let key = format!("key{:010}", i).into_bytes();
            memtable.put(&key, &large_value);
        }

        // Write SST file with small block size to force multiple blocks
        let config = SstConfig { block_size: 512 };
        let writer = SstWriter::new(
            config,
            dir_path.clone(),
            test_name.to_string(),
            memtable.clone(),
        );
        writer.build();

        // Read using SstReader
        let reader = SstReader {
            dir: dir_path,
            filename: test_name.to_string(),
        };

        // Test reading values across block boundaries
        for i in 0..10 {
            let key = format!("key{:010}", i).into_bytes();
            match reader.get(&key) {
                Some(Record::Value(value)) => assert_eq!(&value[..], &large_value),
                _ => panic!("Expected value record for key {}", i),
            }
        }

        // Test reading non-existent keys
        assert!(reader.get(b"nonexistent").is_none());
        assert!(reader.get(b"key0000000010").is_none());
    }

    #[test]
    fn test_get_mixed_records() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();
        let test_name = "test_mixed";

        // Create test data in memtable with a mix of values and tombstones
        let memtable = Arc::new(Memtable::new());
        let num_entries = 1000;

        for i in 0..num_entries {
            let key = format!("key{:010}", i).into_bytes();
            if i % 3 == 0 {
                memtable.delete(&key);
            } else {
                let value = format!("value{:020}", i).into_bytes();
                memtable.put(&key, &value);
            }
        }

        // Write SST file
        let config = SstConfig { block_size: 4096 };
        let writer = SstWriter::new(
            config,
            dir_path.clone(),
            test_name.to_string(),
            memtable.clone(),
        );
        writer.build();

        // Read using SstReader
        let reader = SstReader {
            dir: dir_path,
            filename: test_name.to_string(),
        };

        // Test reading values and tombstones
        for i in 0..num_entries {
            let key = format!("key{:010}", i).into_bytes();
            let result = reader.get(&key);

            if i % 3 == 0 {
                // Should be a tombstone
                match result {
                    Some(Record::Tomb) => (),
                    _ => panic!("Expected tomb record for key {}", i),
                }
            } else {
                // Should be a value
                match result {
                    Some(Record::Value(value)) => {
                        let expected = format!("value{:020}", i);
                        // dbg!(String::from_utf8_lossy(&value));
                        // dbg!(&expected);
                        assert_eq!(&value[..], expected.as_bytes());
                    }
                    _ => panic!("Expected value record for key {}", i),
                }
            }
        }
    }

    #[test]
    fn test_get_binary_data() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();
        let test_name = "test_binary";

        // Create test data in memtable with binary data
        let memtable = Arc::new(Memtable::new());

        // Add binary keys and values
        let binary_pairs = vec![
            (
                vec![0, 255, 10, 13, 32, 1, 2, 3],
                vec![255, 0, 128, 64, 32, 16, 8, 4],
            ),
            (vec![1, 2, 3, 4], vec![5, 6, 7, 8]),
            (vec![255; 10], vec![0; 10]),
        ];

        for (key, value) in binary_pairs.iter() {
            memtable.put(key, value);
        }

        // Write SST file
        let config = SstConfig { block_size: 4096 };
        let writer = SstWriter::new(
            config,
            dir_path.clone(),
            test_name.to_string(),
            memtable.clone(),
        );
        writer.build();

        // Read using SstReader
        let reader = SstReader {
            dir: dir_path,
            filename: test_name.to_string(),
        };

        // Test reading binary data
        for (key, value) in binary_pairs.iter() {
            match reader.get(key) {
                Some(Record::Value(v)) => assert_eq!(&v[..], value),
                _ => panic!("Expected value record for binary key"),
            }
        }
    }
}
