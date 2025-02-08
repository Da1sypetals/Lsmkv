use super::record::Record;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

pub struct Memtable {
    pub(crate) map: SkipMap<Bytes, Record>,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
        }
    }
}

impl Memtable {
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.map.insert(Bytes::copy_from_slice(key), value.into());
    }

    /// Semantics: return either
    /// - Terminate search:
    ///   - Some(Record::Tomb) if the key is deleted
    ///   - Some(Record::Value(value)) if the key is present and not deleted
    /// - Continue search:
    ///   - None if the key is not found in this memtable
    pub fn get(&self, key: &[u8]) -> Option<Record> {
        Some(self.map.get(key)?.value().clone())
    }

    pub fn delete(&self, key: &[u8]) {
        self.map.insert(Bytes::copy_from_slice(key), Record::Tomb);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let memtable = Memtable::new();

        // Test put and get
        memtable.put(b"key1", b"value1");
        let result = memtable.get(b"key1");
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().as_search_result().unwrap(),
            Bytes::copy_from_slice(b"value1")
        );

        // Test overwrite
        memtable.put(b"key1", b"value2");
        let result = memtable.get(b"key1");
        assert_eq!(
            result.unwrap().as_search_result().unwrap(),
            Bytes::copy_from_slice(b"value2")
        );

        // Test get non-existent key
        assert!(memtable.get(b"nonexistent").is_none());
    }

    #[test]
    fn test_deletion() {
        let memtable = Memtable::new();

        // Put and then delete
        memtable.put(b"key1", b"value1");
        memtable.delete(b"key1");

        // Get should return Some(Record::Tomb)
        let result = memtable.get(b"key1");
        assert!(result.is_some());
        assert!(result.unwrap().as_search_result().is_none());

        // Delete non-existent key
        memtable.delete(b"nonexistent");
        let result = memtable.get(b"nonexistent");
        assert!(result.is_some());
        assert!(result.unwrap().as_search_result().is_none());
    }

    #[test]
    fn test_empty_keys_and_values() {
        let memtable = Memtable::new();

        // Test empty key
        memtable.put(b"", b"empty_key");
        assert_eq!(
            memtable.get(b"").unwrap().as_search_result().unwrap(),
            Bytes::copy_from_slice(b"empty_key")
        );

        // Test empty value
        memtable.put(b"empty_value", b"");
        assert_eq!(
            memtable
                .get(b"empty_value")
                .unwrap()
                .as_search_result()
                .unwrap(),
            Bytes::copy_from_slice(b"")
        );

        // Test empty key and value
        memtable.put(b"", b"");
        assert_eq!(
            memtable.get(b"").unwrap().as_search_result().unwrap(),
            Bytes::copy_from_slice(b"")
        );
    }

    #[test]
    fn test_large_values() {
        let memtable = Memtable::new();

        // Create a large value (100KB)
        let large_value = vec![b'x'; 100_000];

        memtable.put(b"large_key", &large_value);
        let result = memtable.get(b"large_key");
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_search_result().unwrap(), large_value);
    }

    #[test]
    fn test_concurrent_operations() {
        use std::sync::Arc;
        use std::thread;
        let memtable = Arc::new(Memtable::new());
        let mut handles = vec![];

        // Spawn multiple threads to write and read
        for i in 0..10 {
            let mt = memtable.clone();
            let handle = thread::spawn(move || {
                let key = format!("key{}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();

                mt.put(&key, &value);
                let result = mt.get(&key);
                assert_eq!(result.unwrap().as_search_result().unwrap(), value);
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all values are present
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            let result = memtable.get(&key);
            assert_eq!(result.unwrap().as_search_result().unwrap(), value);
        }
    }
}
