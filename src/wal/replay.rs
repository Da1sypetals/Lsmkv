use super::log_record::LogRecord;
use crate::memory::{memtable::Memtable, types::Record};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};

pub(crate) struct WalReplayer {
    // transaction id -> tempmap
    pub(crate) transactions: HashMap<u64, BTreeMap<Bytes, Record>>,
}

impl WalReplayer {
    pub(crate) fn empty() -> Self {
        Self {
            transactions: HashMap::new(),
        }
    }

    /// Returns the approximate size of this WAL file
    pub(crate) fn replay_file<R: std::io::Read>(
        &mut self,
        file: &mut R,
        memtable: &Memtable,
    ) -> usize {
        let mut type_buf = [0u8; 1];
        let mut size_buf = [0u8; 2];
        let mut u64_buf = [0u8; 8];
        let mut total_size = 0;

        loop {
            // Try to read key size
            match file.read_exact(&mut type_buf) {
                Ok(_) => {
                    let rec_type = type_buf[0];
                    match rec_type {
                        0 => {
                            // encode -----------------------------------------------------
                            // Read timestamp
                            file.read_exact(&mut u64_buf).unwrap();
                            let timestamp = u64::from_le_bytes(u64_buf);

                            // Read key size
                            file.read_exact(&mut size_buf).unwrap();
                            let key_size = u16::from_le_bytes(size_buf);

                            // Read value size
                            file.read_exact(&mut size_buf).unwrap();
                            let value_size = u16::from_le_bytes(size_buf);

                            // Read key
                            let mut key = vec![0u8; key_size as usize];
                            file.read_exact(&mut key).unwrap();

                            // Read value
                            let mut value = vec![0u8; value_size as usize];
                            file.read_exact(&mut value).unwrap();
                            // encode -----------------------------------------------------

                            // Track total size
                            total_size += key_size as usize + value_size as usize;

                            // Insert into memtable
                            memtable.put(&key, &value, timestamp);
                        }
                        1 => {
                            // encode -----------------------------------------------------
                            // Read timestamp
                            file.read_exact(&mut u64_buf).unwrap();
                            let timestamp = u64::from_le_bytes(u64_buf);

                            // Read key size
                            file.read_exact(&mut size_buf).unwrap();
                            let key_size = u16::from_le_bytes(size_buf);

                            // Read key
                            let mut key = vec![0u8; key_size as usize];
                            file.read_exact(&mut key).unwrap();
                            // encode -----------------------------------------------------

                            // Track total size
                            total_size += key_size as usize;

                            // Insert into memtable
                            memtable.delete(&key, timestamp);
                        }
                        2 => {
                            // println!("TX start");
                            // Transaction Start
                            // encode -----------------------------------------------------
                            // Read start timestamp
                            file.read_exact(&mut u64_buf).unwrap();
                            let _start_timestamp = u64::from_le_bytes(u64_buf);

                            // Read transaction id
                            file.read_exact(&mut u64_buf).unwrap();
                            let transaction_id = u64::from_le_bytes(u64_buf);
                            // encode -----------------------------------------------------

                            // TX: Start a transaction
                            self.transactions.insert(transaction_id, BTreeMap::new());
                        }
                        3 => {
                            // println!("TX value");
                            // Transaction Value
                            // encode -----------------------------------------------------
                            // Read transaction id
                            file.read_exact(&mut u64_buf).unwrap();
                            let transaction_id = u64::from_le_bytes(u64_buf);

                            // Read key size
                            file.read_exact(&mut size_buf).unwrap();
                            let key_size = u16::from_le_bytes(size_buf);

                            // Read value size
                            file.read_exact(&mut size_buf).unwrap();
                            let value_size = u16::from_le_bytes(size_buf);

                            // Read key
                            let mut key = vec![0u8; key_size as usize];
                            file.read_exact(&mut key).unwrap();

                            // Read value
                            let mut value = vec![0u8; value_size as usize];
                            file.read_exact(&mut value).unwrap();
                            // encode -----------------------------------------------------

                            // Track total size
                            total_size += key_size as usize + value_size as usize;

                            // TX: Insert value into transaction
                            if let Some(tempmap) = self.transactions.get_mut(&transaction_id) {
                                tempmap.insert(
                                    Bytes::copy_from_slice(&key),
                                    Record::Value(Bytes::copy_from_slice(&value)),
                                );
                            }
                        }
                        4 => {
                            // println!("TX tomb");
                            // Transaction Tomb
                            // encode -----------------------------------------------------
                            // Read transaction id
                            file.read_exact(&mut u64_buf).unwrap();
                            let transaction_id = u64::from_le_bytes(u64_buf);

                            // Read key size
                            file.read_exact(&mut size_buf).unwrap();
                            let key_size = u16::from_le_bytes(size_buf);

                            // Read key
                            let mut key = vec![0u8; key_size as usize];
                            file.read_exact(&mut key).unwrap();
                            // encode -----------------------------------------------------

                            // Track total size
                            total_size += key_size as usize;

                            // TX: Insert tomb to transaction
                            if let Some(tempmap) = self.transactions.get_mut(&transaction_id) {
                                tempmap.insert(Bytes::copy_from_slice(&key), Record::Tomb);
                            }
                        }
                        5 => {
                            // println!("TX end");
                            // Transaction End
                            // encode -----------------------------------------------------
                            // Read end timestamp
                            file.read_exact(&mut u64_buf).unwrap();
                            let end_timestamp = u64::from_le_bytes(u64_buf);

                            // Read transaction id
                            file.read_exact(&mut u64_buf).unwrap();
                            let transaction_id = u64::from_le_bytes(u64_buf);
                            // encode -----------------------------------------------------

                            // TX: Apply all the transaction's operations
                            if let Some(tempmap) = self.transactions.get_mut(&transaction_id) {
                                for (key, record) in tempmap.iter() {
                                    match record {
                                        Record::Value(value) => {
                                            memtable.put(key, value, end_timestamp);
                                        }
                                        Record::Tomb => {
                                            memtable.delete(key, end_timestamp);
                                        }
                                    }
                                }
                            }

                            self.transactions.remove(&transaction_id);
                        }
                        rec_type => {
                            panic!("Unexpected record type: {}", rec_type);
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => panic!("Error reading WAL file: {}", e),
            }
        }

        total_size
    }
}
