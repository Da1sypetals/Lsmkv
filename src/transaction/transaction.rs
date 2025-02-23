use crate::{
    lsmtree::tree::LsmTree,
    memory::types::{Key, Record},
};
use bytes::Bytes;
use std::collections::BTreeMap;

pub struct Transaction<'a> {
    tree: &'a LsmTree,
    temp: BTreeMap<Key, Record>,
    transaction_id: u64,
    start_timestamp: u64,
}

impl<'a> Transaction<'a> {
    pub fn put(&'a self, key: &[u8], value: &[u8]) {
        todo!()
    }

    pub fn get(&'a self, key: &[u8]) -> Option<Bytes> {
        todo!()
    }

    pub fn delete(&'a self, key: &[u8]) {
        todo!()
    }
}
