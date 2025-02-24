use dashmap::DashSet;
use std::{
    fmt::Display,
    sync::{Mutex, atomic::AtomicU64},
};
use uuid::timestamp;

pub struct MarkerGuard<'a> {
    timestamp: u64,
    clock: &'a Clock,
}

impl Drop for MarkerGuard<'_> {
    fn drop(&mut self) {
        self.clock.markers.remove(&self.timestamp);
    }
}

pub struct Clock {
    path: String,
    cur: Mutex<TransactionTimestamp>,
    markers: DashSet<u64>,
}

#[derive(Clone, Copy, Debug)]
pub struct TransactionTimestamp {
    pub(crate) timestamp: u64,
    pub(crate) transaction_id: u64,
}

impl TransactionTimestamp {
    pub fn empty() -> Self {
        Self {
            timestamp: 1,
            transaction_id: 0,
        }
    }
}

impl Display for TransactionTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.timestamp, self.transaction_id)
    }
}

impl Clock {
    pub fn tick(&self) -> u64 {
        // cur += 1; save incremented cur to file
        let mut cur = self.cur.lock().unwrap();
        cur.timestamp += 1;
        std::fs::write(&self.path, cur.to_string()).unwrap();
        cur.timestamp
    }

    pub fn tick_transaction(&self) -> TransactionTimestamp {
        // cur += 1; save incremented cur to file
        let mut cur = self.cur.lock().unwrap();
        cur.timestamp += 1;
        cur.transaction_id += 1;
        std::fs::write(&self.path, cur.to_string()).unwrap();

        // copy
        *cur
    }

    /// Start an atomic operation with this method.
    pub fn tick_and_mark(&self) -> MarkerGuard {
        let timestamp = self.tick();
        self.markers.insert(timestamp);
        MarkerGuard {
            timestamp,
            clock: self,
        }
    }

    pub fn oldest_marker(&self) -> u64 {
        self.markers
            .iter()
            .map(|a| *a)
            .min()
            .unwrap_or(self.cur.lock().unwrap().timestamp - 1)
    }

    pub fn empty(dir: String) -> Self {
        let res = Self {
            path: format!("{}/clock", dir),
            cur: Mutex::new(TransactionTimestamp::empty()),
            markers: DashSet::new(),
        };
        std::fs::write(&res.path, "1 0").unwrap();
        res
    }

    pub fn load(dir: String) -> Self {
        // read from File
        let cur = std::fs::read_to_string(&format!("{}/clock", dir)).unwrap();
        //     write!(f, "{} {}", self.timestamp, self.transaction_id)
        // refer to the format of the file and parse the cur string
        // the two numbers are separated by a space

        // dbg!(&cur);
        let parts: Vec<&str> = cur.split_whitespace().collect();
        let timestamp = parts[0].parse::<u64>().unwrap();
        let transaction_id = parts[1].parse::<u64>().unwrap();

        Self {
            path: format!("{}/clock", dir),
            cur: Mutex::new(TransactionTimestamp {
                timestamp,
                transaction_id,
            }),
            markers: DashSet::new(),
        }
    }
}
