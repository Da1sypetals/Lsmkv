use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use super::sst::read::SstReader;
use crate::{
    config::{disk::DiskConfig, lsm::LsmConfig},
    disk::sst::write::SstWriter,
    lsmtree::signal::{Signal, SignalReturnStatus},
    memory::{memtable::Memtable, record::Record},
};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{read, File},
    io::{BufReader, BufWriter, Read, Write},
    sync::{Arc, RwLock, RwLockWriteGuard},
    thread::JoinHandle,
};

pub struct LevelInner {
    pub(crate) level: usize,
    pub(crate) counter: usize,
    pub(crate) sst_readers: Vec<SstReader>,
}

impl LevelInner {
    pub(crate) fn new(level: usize) -> Self {
        Self {
            level,
            counter: 0,
            sst_readers: Vec::new(),
        }
    }

    pub(crate) fn get_filename(&mut self) -> String {
        self.counter += 1;
        format!("level_{}/sst_{:08}", self.level, self.counter)
    }
}
pub type Level = RwLock<LevelInner>;

pub struct LsmDisk {
    pub(crate) config: LsmConfig,

    // compact
    /// signal to notify compact thread to start
    pub(crate) compact_signal: Arc<Signal>,
    /// compact thread
    pub(crate) compact_handle: RwLock<Option<JoinHandle<()>>>,

    // Level 0, from oldest to newest
    pub(crate) level_0: Level,

    // Level 1, from oldest to newest
    pub(crate) level_1: Level,

    // Level 2, from newest to oldest
    pub(crate) level_2: Level,

    // Level 3, from newest to oldest
    pub(crate) level_3: Level,
}

impl Drop for LsmDisk {
    fn drop(&mut self) {
        if let Some(handle) = self.compact_handle.write().unwrap().take() {
            handle.join().unwrap();
        }
    }
}

// the only API is get.
impl LsmDisk {
    /// Construct an empty disk, creates all subdirs if not exists.
    ///
    /// # Notes
    ///
    /// - This function will create the following subdirs if not exists:
    ///     - `<config.dir>/level_0`
    ///     - `<config.dir>/level_1`
    ///     - `<config.dir>/level_2`
    ///     - `<config.dir>/level_3`
    /// - This function will also start a compact thread.
    /// - The compact thread will wait on `compact_signal` and try compacting
    ///   L0 -> L1 when the number of sst files in L0 reaches
    ///   `config.disk.level_0_threshold`.
    pub fn empty(config: LsmConfig) -> Arc<Self> {
        // create level subdirs if not exists
        std::fs::create_dir_all(format!("{}/level_0", config.dir)).unwrap();
        std::fs::create_dir_all(format!("{}/level_1", config.dir)).unwrap();
        std::fs::create_dir_all(format!("{}/level_2", config.dir)).unwrap();
        std::fs::create_dir_all(format!("{}/level_3", config.dir)).unwrap();

        let disk = Arc::new(Self {
            config: config.clone(),
            compact_signal: Arc::new(Signal::new()),
            compact_handle: RwLock::new(None),
            level_0: RwLock::new(LevelInner::new(0)),
            level_1: RwLock::new(LevelInner::new(1)),
            level_2: RwLock::new(LevelInner::new(2)),
            level_3: RwLock::new(LevelInner::new(3)),
        });

        let disk_clone = disk.clone();

        // construct signal and compact thread
        let compact_signal_clone = disk.compact_signal.clone();

        *disk.compact_handle.write().unwrap() = Some(std::thread::spawn(move || {
            //
            loop {
                let status = compact_signal_clone.wait();
                if status == SignalReturnStatus::Terminated {
                    break;
                }

                let level_0_threshold = disk_clone.config.disk.level_0_threshold;

                // try compact recursively
                // level 0
                if disk_clone.level_0.read().unwrap().sst_readers.len()
                    > disk_clone.config.disk.level_0_threshold
                {
                    // println!("Start compact L0 -> L1 ...");
                    disk_clone.compact(
                        disk_clone.level_0.write().unwrap(),
                        disk_clone.level_1.write().unwrap(),
                        level_0_threshold,
                    );

                    let level_1_threshold =
                        level_0_threshold * disk_clone.config.disk.block_size_multiplier;

                    // level 1
                    if disk_clone.level_1.read().unwrap().sst_readers.len()
                        > disk_clone.config.disk.level_1_threshold
                    {
                        disk_clone.compact(
                            disk_clone.level_1.write().unwrap(),
                            disk_clone.level_2.write().unwrap(),
                            level_1_threshold,
                        );

                        let level_2_threshold =
                            level_1_threshold * disk_clone.config.disk.block_size_multiplier;
                        // level 2
                        if disk_clone.level_2.read().unwrap().sst_readers.len()
                            > disk_clone.config.disk.level_2_threshold
                        {
                            disk_clone.compact(
                                disk_clone.level_2.write().unwrap(),
                                disk_clone.level_3.write().unwrap(),
                                level_2_threshold,
                            );
                        }
                    }
                }
            }
        }));

        disk
    }

    pub fn new(dir: String, config: LsmConfig) -> Self {
        todo!("Properly initialize all readers and counters !");

        Self {
            config,
            compact_signal: todo!(),
            compact_handle: todo!(),
            level_0: todo!(),
            level_1: todo!(),
            level_2: todo!(),
            level_3: todo!(),
        }
    }

    pub(crate) fn add_l0_sst(&self, replath: &str) {
        assert!(
            replath.starts_with("level_0/"),
            "relpath must start with `level_0/`"
        );

        self.level_0.write().unwrap().sst_readers.push(SstReader {
            dir: self.config.dir.clone(),
            filename: replath.to_string(),
        });

        self.compact_signal.set();
    }
}

impl LsmDisk {
    pub fn get(&self, key: &[u8]) -> Option<Record> {
        // Use .rev() to search from newest to oldest
        for sst in self.level_0.read().unwrap().sst_readers.iter().rev() {
            let val = sst.get(key);
            if val.is_some() {
                return val;
            }
        }

        // Use .rev() to search from newest to oldest
        for sst in self.level_1.read().unwrap().sst_readers.iter().rev() {
            let val = sst.get(key);
            if val.is_some() {
                return val;
            }
        }

        // Use .rev() to search from newest to oldest
        for sst in self.level_2.read().unwrap().sst_readers.iter().rev() {
            let val = sst.get(key);
            if val.is_some() {
                return val;
            }
        }

        // Use .rev() to search from newest to oldest
        for sst in self.level_3.read().unwrap().sst_readers.iter().rev() {
            let val = sst.get(key);
            if val.is_some() {
                return val;
            }
        }

        None
    }
}

impl LsmDisk {
    pub(crate) fn compact(
        self: &Arc<Self>,
        from: RwLockWriteGuard<LevelInner>,
        mut to: RwLockWriteGuard<LevelInner>,
        threshold: usize,
    ) {
        let mut approx_size = 0;
        let mut map: SkipMap<Bytes, Record> = SkipMap::new();

        // Iterate files from latest to oldest:
        // 1. If the key does not exist in the map, insert key-record_size pair into the keys map;
        // 2. update the approximate size;
        // 3. if the approx size reaches the threshold, flush the btreemap content into a sst. clear the btreemap.
        let files: Vec<_> = from
            .sst_readers
            .iter()
            .rev()
            .map(|sst| {
                std::fs::OpenOptions::new()
                    .read(true)
                    .open(sst.sst_path())
                    .unwrap()
            })
            .collect();

        let mut readers: Vec<_> = files.iter().map(|f| BufReader::new(f)).collect();

        let sizes: Vec<_> = files
            .iter()
            .map(|f| f.metadata().unwrap().len() as usize)
            .collect();

        let mut cursors: Vec<_> = vec![0_usize; from.sst_readers.len()];
        let mut keys = vec![vec![]; from.sst_readers.len()];

        while !cursors
            .iter()
            .zip(sizes.iter())
            .all(|(cursor, size)| *cursor >= *size as usize)
        {
            // debug print all cursor positions and its corresponding sizes
            // for (i, (cursor, size)) in cursors.iter().zip(sizes.iter()).enumerate() {
            //     println!("File: {}, cursor: {}, size: {}", i, cursor, size);
            // }
            // println!("");

            // while not all files got to  its end {
            // 1. move the latest file's cursor by 1 kv pair, insert into btreemap
            // 2. from new to old (except the first one):
            //    while ith-cursor-key < i-1th-cursor-key, advance 1 kv pair, if not in btreemap, insert it, else do nothing.

            if cursors[0] < sizes[0] {
                let kv_0 = read_kv(&mut readers[0]);
                approx_size += kv_0.size();
                cursors[0] += kv_0.size();
                keys[0] = kv_0.key.clone();
                map.insert(Bytes::copy_from_slice(&kv_0.key), kv_0.value);
            }

            for (i, reader) in readers.iter_mut().enumerate().skip(1) {
                let mut kv;
                // wtf is this syntax...
                while cursors[i] < sizes[i] && {
                    kv = read_kv(reader);
                    reader.seek_relative(-(kv.size() as i64)).unwrap();
                    kv.key <= keys[i - 1]
                } {
                    // dbg!(String::from_utf8_lossy(&kv.key));

                    keys[i] = kv.key.clone();
                    cursors[i] += kv.size();
                    // if not exist, insert, else do not insert.
                    map.get_or_insert_with(Bytes::copy_from_slice(&kv.key), || {
                        // insertion operation
                        approx_size += kv.size();

                        kv.value
                    });

                    if approx_size > threshold {
                        let relpath = to.get_filename();
                        // close previous sst file
                        let writer = SstWriter::new(
                            self.config.sst.clone(),
                            self.config.dir.clone(),
                            relpath.clone(),
                            Memtable { map }.into(),
                        );

                        map = SkipMap::new();

                        to.sst_readers.push(SstReader {
                            dir: self.config.dir.clone(),
                            filename: relpath,
                        });
                    }
                }
            }
        }
    }
}

pub struct Kv {
    key: Vec<u8>,
    value: Record,
}

impl Kv {
    pub fn size(&self) -> usize {
        match &self.value {
            Record::Value(value) => 1 + 2 + 2 + self.key.len() + value.len(),
            Record::Tomb => 1 + 2 + self.key.len(),
        }
    }
}

fn read_kv(reader: &mut BufReader<&File>) -> Kv {
    // Read record type
    let mut buf = Vec::new();
    buf.resize(1, 0);

    // reader.read_exact(&mut buf).unwrap();
    match reader.read_exact(&mut buf) {
        Ok(_) => (),
        Err(e) => {
            println!("Error reading record type: {}", e);
            println!("File size: {}", reader.get_ref().metadata().unwrap().len());
            panic!("Error reading record type");
        }
    }

    let record_type = buf[0];
    // println!("Reading record type: {}", record_type);

    match record_type {
        /*
        If record is value:
            if key match, retrieve value;
            else, seek to next kv pair.
        */
        0 => {
            // Value record
            // Read key size
            buf.resize(2, 0);
            reader.read_exact(&mut buf).unwrap();
            let key_size = u16::from_le_bytes(buf.to_vec().try_into().unwrap()) as usize;
            // println!("Value record: key_size = {}", key_size);

            // Read value size
            buf.resize(2, 0);
            reader.read_exact(&mut buf).unwrap();
            let value_size = u16::from_le_bytes(buf.to_vec().try_into().unwrap()) as usize;
            // println!("Value record: value_size = {}", value_size);

            // Read key
            let mut key = Vec::new();
            buf.resize(key_size, 0);
            reader.read_exact(&mut buf).unwrap();
            key = buf.clone();
            // println!("Read key: {:?}", String::from_utf8_lossy(&key));

            let mut value = Vec::new();
            buf.resize(value_size, 0);
            reader.read_exact(&mut buf).unwrap();
            value = buf;

            Kv {
                key,
                value: Record::Value(Bytes::copy_from_slice(&value)),
            }
        }
        1 => {
            // Tomb record
            // Read key size
            buf.resize(2, 0);
            reader.read_exact(&mut buf).unwrap();
            let key_size = u16::from_le_bytes(buf.to_vec().try_into().unwrap()) as usize;
            // println!("Tomb record: key_size = {}", key_size);

            // Read key
            let mut key = Vec::new();
            buf.resize(key_size, 0);
            reader.read_exact(&mut buf).unwrap();
            key = buf;
            // println!("Read key: {:?}", String::from_utf8_lossy(&key));

            Kv {
                key,
                value: Record::Tomb,
            }
        }
        type_id => panic!("Invalid record type: type_id = {}", type_id),
    }
}
