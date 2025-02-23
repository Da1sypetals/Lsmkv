use dashmap::DashSet;
use std::sync::{Mutex, atomic::AtomicU64};

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
    cur: Mutex<u64>,
    markers: DashSet<u64>,
}

impl Clock {
    pub fn tick(&self) -> u64 {
        // cur += 1; save incremented cur to file
        let mut cur = self.cur.lock().unwrap();
        *cur += 1;
        let cur = *cur;
        std::fs::write(&self.path, cur.to_string()).unwrap();
        cur
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
            .unwrap_or(*self.cur.lock().unwrap() - 1)
    }

    pub fn empty(dir: String) -> Self {
        Self {
            path: format!("{}/clock", dir),
            cur: Mutex::new(1),
            markers: DashSet::new(),
        }
    }

    pub fn new(dir: String) -> Self {
        // read from File
        let cur = std::fs::read_to_string(&format!("{}/clock", dir)).unwrap();
        let cur = cur.parse::<u64>().unwrap();

        Self {
            path: format!("{}/clock", dir),
            cur: Mutex::new(cur),
            markers: DashSet::new(),
        }
    }
}
