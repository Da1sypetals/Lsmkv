use crate::memory::types::Key;
use bytes::Bytes;
use crc32fast::Hasher;
use farmhash::fingerprint64;

pub(crate) type Index = Vec<(Key, u64)>;

/// Bloom filter only checks membership of key.
/// It discards timestamp.
/// This matches the behavior of bloom filter that has no false negative.
pub struct BloomFilter {
    bits: Vec<u8>,   // Bit storage array
    k: u32,          // Number of hash functions
    num_bits: usize, // Total number of bits
}

impl BloomFilter {
    /// Create a new Bloom filter
    /// - num_bits: Total number of bits (calculated based on a false positive rate of 0.01)[^1]
    /// - k: Number of hash functions (default is 7)[^3]
    pub fn new(num_bits: usize, k: u32) -> Self {
        let num_bytes = (num_bits + 7) / 8;
        BloomFilter {
            bits: vec![0; num_bytes],
            k,
            num_bits,
        }
    }

    /// Create a new Bloom filter, automatically calculating parameters based on the expected number of elements and the acceptable false positive rate.
    ///
    /// - `scale`: Expected number of elements to be inserted.
    /// - `fpr`: Acceptable false positive rate (e.g., 0.01 for 1%).
    pub fn with_scale_and_fpr(scale: usize, fpr: f64) -> Self {
        // Calculate the required number of bits (num_bits)
        let num_bits = (-(scale as f64) * fpr.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;

        // Calculate the required number of hash functions (k)
        let k = ((num_bits as f64 / scale as f64) * 2.0_f64.ln()).ceil() as u32;

        // Call the existing constructor
        Self::new(num_bits, k)
    }

    /// Insert a key
    pub fn insert(&mut self, key: &[u8]) {
        let mut h = fingerprint64(key);
        let delta = h >> 33 | h << 31;

        for _ in 0..self.k {
            let bit_idx = (h % self.num_bits as u64) as usize;
            self.set_bit(bit_idx);
            h = h.wrapping_add(delta);
        }
    }

    /// Check if a key exists
    pub fn contains(&self, key: &[u8]) -> bool {
        let mut h = fingerprint64(key);
        let delta = h >> 33 | h << 31;

        for _ in 0..self.k {
            let bit_idx = (h % self.num_bits as u64) as usize;
            if !self.get_bit(bit_idx) {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }

    /// Encode as a byte stream (with checksum)
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + 4 + self.bits.len() + 4);
        buf.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        buf.extend_from_slice(&self.k.to_le_bytes());
        buf.extend_from_slice(&self.bits);

        let mut hasher = Hasher::new();
        hasher.update(&buf);
        let checksum = hasher.finalize();
        buf.extend_from_slice(&checksum.to_le_bytes());

        buf
    }

    /// Decode a byte stream (verify checksum)
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 12 {
            return None;
        }

        let checksum = u32::from_le_bytes(data[data.len() - 4..].to_vec().try_into().unwrap());
        let mut hasher = Hasher::new();
        hasher.update(&data[..data.len() - 4]);

        if hasher.finalize() != checksum {
            return None;
        }

        let num_bits = u32::from_le_bytes(data[0..4].to_vec().try_into().unwrap()) as usize;
        let k = u32::from_le_bytes(data[4..8].to_vec().try_into().unwrap());
        let bits = data[8..data.len() - 4].to_vec();

        Some(BloomFilter { bits, k, num_bits })
    }

    // Private helper methods
    fn set_bit(&mut self, idx: usize) {
        let byte_idx = idx / 8;
        let bit_idx = idx % 8;
        self.bits[byte_idx] |= 1 << bit_idx;
    }

    fn get_bit(&self, idx: usize) -> bool {
        let byte_idx = idx / 8;
        let bit_idx = idx % 8;
        (self.bits[byte_idx] & (1 << bit_idx)) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic_operations() {
        let mut filter = BloomFilter::new(1000, 7);

        // Test insertion and positive lookups
        let key1 = b"test_key_1";
        let key2 = b"test_key_2";
        filter.insert(key1);
        filter.insert(key2);

        assert!(filter.contains(key1));
        assert!(filter.contains(key2));

        // Test negative lookup
        let non_existent_key = b"non_existent_key";
        assert!(!filter.contains(non_existent_key));
    }

    #[test]
    fn test_bloom_filter_encoding_decoding() {
        let mut original = BloomFilter::new(1000, 7);
        let test_keys = vec![b"key1", b"key2", b"key3"];

        // Insert some test data
        for &key in &test_keys {
            original.insert(key);
        }

        // Encode and decode
        let encoded = original.encode();
        let decoded = BloomFilter::decode(&encoded).expect("Failed to decode valid filter");

        // Verify decoded filter maintains same properties
        assert_eq!(decoded.k, original.k);
        assert_eq!(decoded.num_bits, original.num_bits);
        assert_eq!(decoded.bits, original.bits);

        // Verify functionality is preserved
        for &key in &test_keys {
            assert!(decoded.contains(key));
        }
    }

    #[test]
    fn test_bloom_filter_encoding_decoding_large_dataset() {
        let mut original = BloomFilter::new(1000000, 15);
        let num_keys = 10000;
        let some_random_number = num_keys + 23905;
        let test_keys: Vec<_> = (1..=num_keys)
            .map(|i| {
                format!("key-{}-{}", i, some_random_number - i)
                    .as_bytes()
                    .to_vec()
            })
            .collect();

        // Insert some test data
        for key in &test_keys {
            original.insert(key);
        }

        // Encode and decode
        let encoded = original.encode();
        let decoded = BloomFilter::decode(&encoded).expect("Failed to decode valid filter");

        // Verify decoded filter maintains same properties
        assert_eq!(decoded.k, original.k);
        assert_eq!(decoded.num_bits, original.num_bits);
        assert_eq!(decoded.bits, original.bits);

        // Verify functionality is preserved
        for key in &test_keys {
            assert!(decoded.contains(key));
        }
    }

    #[test]
    fn test_bloom_filter_invalid_decode() {
        // Test with invalid data length
        let invalid_data = vec![1, 2, 3]; // Too short
        assert!(BloomFilter::decode(&invalid_data).is_none());

        // Test with invalid checksum
        let mut filter = BloomFilter::new(100, 7);
        filter.insert(b"test");
        let mut encoded = filter.encode();
        // Corrupt the last 4 bytes (checksum)
        if let Some(last) = encoded.last_mut() {
            *last ^= 0xFF;
        }
        assert!(BloomFilter::decode(&encoded).is_none());
    }

    #[test]
    fn test_bloom_filter_false_positives() {
        let mut filter = BloomFilter::new(100, 7); // Small size to increase false positive rate

        // Insert some keys
        for i in 0..10 {
            filter.insert(format!("key{}", i).as_bytes());
        }

        // Test some keys that weren't inserted
        // Note: This test demonstrates that false positives are possible
        let mut false_positives = 0;
        for i in 100..200 {
            if filter.contains(format!("key{}", i).as_bytes()) {
                false_positives += 1;
            }
        }

        // With these parameters, we expect some false positives
        println!("False positive count: {}", false_positives);
    }

    #[test]
    fn test_bloom_filter_edge_cases() {
        // Test with empty key
        let mut filter = BloomFilter::new(1000, 7);
        filter.insert(b"");
        assert!(filter.contains(b""));

        // Test with very large key
        let large_key = vec![0u8; 1000];
        filter.insert(&large_key);
        assert!(filter.contains(&large_key));

        // Test minimum size filter
        let min_filter = BloomFilter::new(8, 1);
        min_filter.encode(); // Should not panic
    }

    #[test]
    fn test_bloom_filter_large_dataset() {
        // Create a Bloom filter, assuming we expect to insert 100,000 keys
        let num_bits = 1_000_000; // Choose an appropriate number of bits based on the false positive rate
        let k = 7; // Number of hash functions
        let mut filter = BloomFilter::new(num_bits, k);

        // Insert 100,000 keys
        let num_keys = 100_000;
        for i in 0..num_keys {
            let key = format!("key_{}", i).into_bytes();
            filter.insert(&key);
        }

        // Check if all inserted keys are present in the filter
        for i in 0..num_keys {
            let key = format!("key_{}", i).into_bytes();
            assert!(filter.contains(&key), "Key {} should be in the filter", i);
        }

        // Check some keys that were not inserted to ensure the false positive rate is within the expected range
        let mut false_positives = 0;
        let num_checks = 10_000;
        for i in num_keys..num_keys + num_checks {
            let key = format!("key_{}", i).into_bytes();
            if filter.contains(&key) {
                false_positives += 1;
            }
        }

        // Output the false positive rate
        let false_positive_rate = false_positives as f64 / num_checks as f64;
        println!("False positive rate: {:.4}%", false_positive_rate * 100.0);

        // According to Bloom filter theory, the false positive rate should be below a certain threshold
        // Here we assume the false positive rate should be below 1%
        assert!(
            false_positive_rate < 0.03,
            "False positive rate is too high"
        );
    }
}
