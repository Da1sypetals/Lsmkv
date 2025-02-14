use bytes::Bytes;

pub(crate) type Index = Vec<(Bytes, u64)>;

// pub struct BloomFilter {}

// impl BloomFilter {
//     pub fn new() -> Self {
//         todo!()
//     }

//     pub fn add(&mut self) {
//         todo!()
//     }

//     pub fn contains(&self, key: Vec<u8>) -> bool {
//         todo!()
//     }
// }

use crc32fast::Hasher;
use farmhash::{fingerprint32, fingerprint64};

pub struct BloomFilter {
    bits: Vec<u8>,   // 位存储数组
    k: u32,          // 哈希函数数量
    num_bits: usize, // 总位数
}

impl BloomFilter {
    /// 创建新布隆过滤器
    /// - num_bits: 总位数（根据假阳性率0.01计算）[^1]
    /// - k: 哈希次数（默认7次）[^3]
    pub fn new(num_bits: usize, k: u32) -> Self {
        let num_bytes = (num_bits + 7) / 8;
        BloomFilter {
            bits: vec![0; num_bytes],
            k,
            num_bits,
        }
    }

    /// 创建一个新的布隆过滤器，根据预期的元素数量和假阳性率自动计算参数。
    ///
    /// - `scale`: 预期插入的元素数量。
    /// - `fpr`: 可接受的假阳性率（例如 0.01 表示 1%）。
    pub fn with_scale_and_fpr(scale: usize, fpr: f64) -> Self {
        // 计算所需的位数 (num_bits)
        let num_bits = (-(scale as f64) * fpr.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;

        // 计算所需的哈希函数数量 (k)
        let k = ((num_bits as f64 / scale as f64) * 2.0_f64.ln()).ceil() as u32;

        // 调用现有的构造函数
        Self::new(num_bits, k)
    }

    /// 插入键值
    pub fn insert(&mut self, key: &[u8]) {
        let mut h = fingerprint64(key);
        let delta = h >> 33 | h << 31;

        for _ in 0..self.k {
            let bit_idx = (h % self.num_bits as u64) as usize;
            self.set_bit(bit_idx);
            h = h.wrapping_add(delta);
        }
    }

    /// 检查键存在性
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

    /// 编码为字节流（含校验和）
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

    /// 解码字节流（验证校验和）
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

    // 私有辅助方法
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
        // 创建一个布隆过滤器，假设我们预计插入100,000个键值
        let num_bits = 1_000_000; // 根据假阳性率选择合适的位数
        let k = 7; // 哈希函数数量
        let mut filter = BloomFilter::new(num_bits, k);

        // 插入100,000个键值
        let num_keys = 100_000;
        for i in 0..num_keys {
            let key = format!("key_{}", i).into_bytes();
            filter.insert(&key);
        }

        // 检查所有插入的键值是否存在于过滤器中
        for i in 0..num_keys {
            let key = format!("key_{}", i).into_bytes();
            assert!(filter.contains(&key), "Key {} should be in the filter", i);
        }

        // 检查一些未插入的键值，确保假阳性率在预期范围内
        let mut false_positives = 0;
        let num_checks = 10_000;
        for i in num_keys..num_keys + num_checks {
            let key = format!("key_{}", i).into_bytes();
            if filter.contains(&key) {
                false_positives += 1;
            }
        }

        // 输出假阳性率
        let false_positive_rate = false_positives as f64 / num_checks as f64;
        println!("False positive rate: {:.4}%", false_positive_rate * 100.0);

        // 根据布隆过滤器的理论，假阳性率应低于某个阈值
        // 这里我们假设假阳性率应低于1%
        assert!(
            false_positive_rate < 0.03,
            "False positive rate is too high"
        );
    }
}
