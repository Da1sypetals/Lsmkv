use bytes::Bytes;
use std::{borrow::Borrow, cmp::Ordering};

// ########################### key ###########################

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Key {
    pub key: Bytes,
    pub timestamp: u64,
}

impl Key {
    pub fn new(key: Bytes, timestamp: u64) -> Self {
        Self { key, timestamp }
    }

    pub fn from_slice(slice: &[u8], timestamp: u64) -> Self {
        let key = Bytes::copy_from_slice(&slice);
        Self { key, timestamp }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        // first compare keys
        let key_cmp = self.key.cmp(&other.key);
        if key_cmp != Ordering::Equal {
            return key_cmp;
        }

        // then compare timestamps and revert the order
        // Note that timestamp is sorted in DESCENDING order
        other.timestamp.cmp(&self.timestamp)
    }
}

// ########################### value ###########################

#[derive(Clone, Debug, PartialEq)]
pub enum Record {
    Value(Bytes),
    Tomb,
}

impl Record {
    pub fn as_search_result(self) -> Option<Bytes> {
        match self {
            Record::Value(bytes) => Some(bytes),
            Record::Tomb => None,
        }
    }
}

impl Into<Record> for Bytes {
    fn into(self) -> Record {
        Record::Value(self)
    }
}

impl Into<Record> for &[u8] {
    fn into(self) -> Record {
        Record::Value(Bytes::copy_from_slice(self))
    }
}
