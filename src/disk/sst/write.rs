use crate::disk::sst::Index;
use crate::{
    config::sst::SstConfig,
    disk::block::DataBlockWriter,
    memory::{memtable::Memtable, record::Record},
};
use bytes::Bytes;
use std::{
    fs::File,
    io::{BufWriter, Seek, Write},
    sync::Arc,
};

use super::search::BloomFilter;

pub struct SstWriter {
    config: SstConfig,
    memtable: Arc<Memtable>,
    dir: String,
    relpath: String,
}

impl SstWriter {
    #[must_use]
    pub fn new(config: SstConfig, dir: String, relpath: String, memtable: Arc<Memtable>) -> Self {
        Self {
            config,
            memtable,
            dir,
            relpath,
        }
    }

    pub fn build(self) {
        // println!("Building SST file: {}/{}.data", self.dir, self.relpath);
        let mut datafile = File::create(&format!("{}/{}.data", self.dir, self.relpath)).unwrap();
        datafile.seek(std::io::SeekFrom::Start(0)).unwrap();

        let mut index = Index::new();
        // BF: bloom filter init here
        let mut bloomfilter = BloomFilter::with_scale_and_fpr(self.config.scale, self.config.fpr);

        let mut cur_block = DataBlockWriter::new(&mut datafile, 0);

        let mut offset = 0;
        let mut last_kv_size = 0;

        for kv in &self.memtable.map {
            if cur_block.len() == 0 {
                // add to index
                index.push((kv.key().clone(), offset));
                // println!("Added index entry at offset {}: {:?}", offset, kv.key());
            }

            // BF: add to bloom filter here
            // dbg!(kv.key());
            bloomfilter.insert(kv.key());

            // Write to disk via block
            last_kv_size = cur_block.append(kv.key(), kv.value());
            offset += last_kv_size;

            if cur_block.size() > self.config.block_size as u64 {
                // println!(
                //     "Block size {} exceeded threshold {}, flushing",
                //     cur_block.size(),
                //     self.config.block_size
                // );
                // flush current block via drop
                drop(cur_block);

                // create new current block
                cur_block = DataBlockWriter::new(&mut datafile, offset);
            }
        }

        // the last key
        let memtable_last = self.memtable.map.iter().last().unwrap();
        if index.last().unwrap().0 != memtable_last.key() {
            let last_key_offset = offset - last_kv_size;
            index.push((memtable_last.key().clone(), last_key_offset));
        }

        // index file
        let indexfile = File::create(&format!("{}/{}.index", self.dir, self.relpath)).unwrap();
        let mut index_writer = BufWriter::new(indexfile);
        for (key, datafile_offset) in index {
            let key_len = key.len() as u16;
            index_writer.write_all(&key_len.to_le_bytes()).unwrap();
            index_writer.write_all(&key).unwrap();
            index_writer
                .write_all(&datafile_offset.to_le_bytes())
                .unwrap();
        }
        index_writer.flush().unwrap();

        // BF: Bloom filter file
        let bloomfilterfile = File::create(&format!("{}/{}.bf", self.dir, self.relpath)).unwrap();
        let mut bloomfilter_writer = BufWriter::new(bloomfilterfile);
        bloomfilter_writer.write_all(&bloomfilter.encode()).unwrap();
        bloomfilter_writer.flush().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::record::Record;
    use crossbeam_skiplist::SkipMap;
    use std::io::Read;
    use tempfile::tempdir;

    fn create_large_test_memtable(num_entries: usize) -> Arc<Memtable> {
        let map = SkipMap::new();
        for i in 0..num_entries {
            let key = format!("key{:010}", i); // Fixed-width keys for easier verification
            let value = format!("value{:020}", i); // Fixed-width values
            map.insert(Bytes::from(key), Record::Value(Bytes::from(value)));
        }
        Arc::new(Memtable { map })
    }

    fn read_entire_file(path: &str) -> Vec<u8> {
        let mut file = File::open(path).unwrap();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).unwrap();
        contents
    }

    #[test]
    fn test_sst_writer_large_dataset() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();

        let config = SstConfig {
            block_size: 4096, // Standard page size
            scale: 100,
            fpr: 0.01,
        };

        // Create memtable with 1000 entries
        let num_entries = 1000;
        let memtable = create_large_test_memtable(num_entries);
        let writer = SstWriter::new(
            config,
            dir_path.clone(),
            "test_large".to_string(),
            memtable.clone(),
        );

        writer.build();

        // Read and verify data file
        let data_path = format!("{}/test_large.data", dir_path);
        let data_contents = read_entire_file(&data_path);

        // Read and verify index file
        let index_path = format!("{}/test_large.index", dir_path);
        let index_contents = read_entire_file(&index_path);

        // Verify index file structure
        let mut index_pos = 0;
        let mut prev_offset = 0;
        let mut num_index_entries = 0;

        while index_pos < index_contents.len() {
            // Read key length (u16)
            let key_len =
                u16::from_le_bytes(index_contents[index_pos..index_pos + 2].try_into().unwrap())
                    as usize;
            index_pos += 2;

            dbg!(key_len);

            // Read key
            let key = &index_contents[index_pos..index_pos + key_len];
            let key_str = String::from_utf8_lossy(key);
            assert!(key_str.starts_with("key")); // Verify key format

            dbg!(&key_str);

            // Verify key length matches the stored length
            assert_eq!(
                key_len,
                key_str.len(),
                "Key length in index file doesn't match actual key length"
            );

            // Verify key format and length matches our fixed-width format (3 for "key" + 10 for number)
            assert_eq!(
                key_len, 13,
                "Key length should be 13 (key + 10 digit number)"
            );

            index_pos += key_len;

            // Read offset (u64)
            let offset =
                u64::from_le_bytes(index_contents[index_pos..index_pos + 8].try_into().unwrap());
            index_pos += 8;

            dbg!(offset);

            // Verify offset is increasing
            assert!(
                offset >= prev_offset,
                "Offsets should be monotonically increasing"
            );
            prev_offset = offset;
            num_index_entries += 1;
        }

        // Verify data file structure
        let mut data_pos = 0;
        let mut num_records = 0;

        while data_pos < data_contents.len() {
            // Read record type (1 byte)
            let record_type = data_contents[data_pos];
            assert_eq!(record_type, 0, "All records should be Value type"); // 0 for Value type
            data_pos += 1;

            // Read key size (2 bytes)
            let key_size =
                u16::from_le_bytes(data_contents[data_pos..data_pos + 2].try_into().unwrap())
                    as usize;
            data_pos += 2;

            // Read value size (2 bytes)
            let value_size =
                u16::from_le_bytes(data_contents[data_pos..data_pos + 2].try_into().unwrap())
                    as usize;
            data_pos += 2;

            // Verify key size matches our fixed-width format
            assert_eq!(
                key_size, 13,
                "Key size in data file should be 13 (key + 10 digit number)"
            );

            // Read key
            let key = &data_contents[data_pos..data_pos + key_size];
            let key_str = String::from_utf8_lossy(key);
            assert!(key_str.starts_with("key")); // Verify key format
            assert_eq!(
                key_size,
                key_str.len(),
                "Key size in data file doesn't match actual key length"
            );
            data_pos += key_size;

            // Verify value size matches our fixed-width format (5 for "value" + 20 for number)
            assert_eq!(
                value_size, 25,
                "Value size should be 25 (value + 20 digit number)"
            );

            // Read value
            let value = &data_contents[data_pos..data_pos + value_size];
            let value_str = String::from_utf8_lossy(value);
            assert!(value_str.starts_with("value")); // Verify value format
            assert_eq!(
                value_size,
                value_str.len(),
                "Value size in data file doesn't match actual value length"
            );
            data_pos += value_size;

            // println!("{}: {}", key_str, value_str);

            num_records += 1;
        }

        // Verify we found all records
        assert_eq!(
            num_records, num_entries,
            "Number of records in data file should match input"
        );

        // Verify the first few entries in detail
        let mut index_pos = 0;
        for i in 0..5 {
            let key_len =
                u16::from_le_bytes(index_contents[index_pos..index_pos + 2].try_into().unwrap())
                    as usize;
            index_pos += 2;

            let key = &index_contents[index_pos..index_pos + key_len];
            let expected_key = format!("key{:010}", i);
            // assert_eq!(String::from_utf8_lossy(key), expected_key);
            println!("Index: {}", String::from_utf8_lossy(key));
            let key_str = String::from_utf8_lossy(key);
            let key_num = &key_str[3..].parse::<usize>().unwrap();
            assert_eq!(*key_num, 96 * i);
            index_pos += key_len;

            // Skip offset verification as it's implementation dependent
            index_pos += 8;
        }
    }

    #[test]
    fn test_sst_writer_block_boundaries() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();

        // Use very small block size to force frequent block splits
        let config = SstConfig {
            block_size: 64, // Small block size
            scale: 100,
            fpr: 0.01,
        };

        // Create memtable with entries that will definitely cross block boundaries
        let memtable = create_large_test_memtable(50);
        let writer = SstWriter::new(
            config,
            dir_path.clone(),
            "test_blocks".to_string(),
            memtable,
        );

        writer.build();

        // Read and verify index file
        let index_path = format!("{}/test_blocks.index", dir_path);
        let index_contents = read_entire_file(&index_path);

        // Count number of index entries (each representing a block)
        let mut num_blocks = 0;
        let mut pos = 0;
        while pos < index_contents.len() {
            let key_len =
                u16::from_le_bytes(index_contents[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2 + key_len + 8; // Skip key length, key, and offset
            num_blocks += 1;
        }

        // With small block size, we should have multiple blocks
        dbg!(num_blocks);
        assert!(
            num_blocks > 1,
            "Should have multiple blocks with small block size"
        );
    }
}
