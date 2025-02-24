use crate::{
    lsmtree::tree::LsmTree,
    memory::types::{Key, Record},
    wal::log_record::LogRecord,
};
use bytes::Bytes;
use std::collections::BTreeMap;
use uuid::timestamp;

pub struct Transaction<'a> {
    pub(crate) tree: &'a LsmTree,
    pub(crate) tempmap: BTreeMap<Bytes, Record>,
    pub(crate) transaction_id: u64,
    pub(crate) start_timestamp: u64,
}

impl<'a> Transaction<'a> {
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.tempmap.insert(
            Bytes::copy_from_slice(key),
            Record::Value(Bytes::copy_from_slice(value)),
        );
    }

    pub fn get(&'a self, key: &[u8]) -> Option<Bytes> {
        match self.tempmap.get(key) {
            Some(record) => match record {
                Record::Value(value) => Some(value.clone()),
                Record::Tomb => None,
            },
            None => self.tree.get_by_time(key, self.start_timestamp),
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.tempmap
            .insert(Bytes::copy_from_slice(key), Record::Tomb);
    }
}

impl<'a> Transaction<'a> {
    pub fn commit(self) {
        let end_timestamp = self.tree.clock.tick();
        // write WAL
        {
            let mut wal = self.tree.wal.write().unwrap();
            wal.log(LogRecord::TransactionEnd {
                end_timestamp,
                transaction_id: self.transaction_id,
            });
        }

        // write to tree
        let mem = self.tree.mem.read().unwrap();
        for (key_bytes, value) in self.tempmap {
            // println!("value {}, wal lock", String::from_utf8_lossy(value));
            let timestamp = self.tree.clock.tick();
            // dbg!(timestamp);

            // 2. write to memory.
            // println!("value {}, mem lock", String::from_utf8_lossy(value));
            match value {
                Record::Value(value_bytes) => {
                    mem.put(&key_bytes, &value_bytes, timestamp);
                }
                Record::Tomb => {
                    mem.delete(&key_bytes, timestamp);
                }
            }
        }
    }
}
