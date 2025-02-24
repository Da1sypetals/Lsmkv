use super::{
    manifest::{LevelManifest, Manifest},
    sst::read::SstReader,
};
use crate::{
    clock::Clock,
    config::lsm::LsmConfig,
    disk::sst::write::SstWriter,
    lsmtree::signal::{Signal, SignalReturnStatus},
    memory::{
        memtable::Memtable,
        types::{Key, Record},
    },
};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use scc::HashMap;
use std::{
    collections::HashSet,
    fs::{self, File},
    io::{BufReader, Read},
    sync::{Arc, RwLock, RwLockWriteGuard},
    thread::JoinHandle,
    u64,
};

pub enum CompactionLevel {
    L01,
    L12,
    L23,
}

impl CompactionLevel {
    pub fn from_to_levels<'a>(
        &self,
        disk: &'a Arc<LsmDisk>,
    ) -> (
        RwLockWriteGuard<'a, LevelInner>,
        RwLockWriteGuard<'a, LevelInner>,
    ) {
        match self {
            CompactionLevel::L01 => {
                let from = disk.level_0.write().unwrap();
                let to = disk.level_1.write().unwrap();
                (from, to)
            }
            CompactionLevel::L12 => {
                let from = disk.level_1.write().unwrap();
                let to = disk.level_2.write().unwrap();
                (from, to)
            }
            CompactionLevel::L23 => {
                let from = disk.level_2.write().unwrap();
                let to = disk.level_3.write().unwrap();
                (from, to)
            }
        }
    }
}

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
    pub fn empty(config: LsmConfig, clock: &Arc<Clock>) -> Arc<Self> {
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

        disk.init_compact_thread(&clock);

        disk
    }

    pub fn load(config: LsmConfig, clock: &Arc<Clock>) -> Arc<Self> {
        let manifest: Manifest =
            toml::from_str(&fs::read_to_string(&format!("{}/manifest.toml", &config.dir)).unwrap())
                .unwrap();

        let disk = Arc::new(Self {
            compact_signal: Arc::new(Signal::new()),
            compact_handle: RwLock::new(None),
            level_0: manifest.level_0.as_level(&config.dir, 0),
            level_1: manifest.level_1.as_level(&config.dir, 1),
            level_2: manifest.level_2.as_level(&config.dir, 2),
            level_3: manifest.level_3.as_level(&config.dir, 3),
            config,
        });

        disk.init_compact_thread(clock);

        disk
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

    fn init_compact_thread(self: &Arc<Self>, clock: &Arc<Clock>) {
        let self_clone = self.clone();
        let clock_clone = clock.clone();

        // construct signal and compact thread
        let compact_signal_clone = self.compact_signal.clone();

        if self.config.disk.auto_compact {
            *self.compact_handle.write().unwrap() = Some(std::thread::spawn(move || {
                //
                loop {
                    let status = compact_signal_clone.wait();
                    if status == SignalReturnStatus::Terminated {
                        break;
                    }

                    let oldest_timestamp = clock_clone.oldest_marker();

                    let level_0_size_threshold = self_clone.config.disk.level_0_size_threshold;

                    // try compact recursively
                    // level 0
                    if self_clone.level_0.read().unwrap().sst_readers.len()
                        > self_clone.config.disk.level_0_threshold
                    {
                        println!("Start compact L0 -> L1 ...");
                        self_clone.compact(
                            CompactionLevel::L01,
                            level_0_size_threshold,
                            oldest_timestamp,
                        );

                        let level_1_size_threshold =
                            level_0_size_threshold * self_clone.config.disk.block_size_multiplier;

                        // level 1
                        if self_clone.level_1.read().unwrap().sst_readers.len()
                            > self_clone.config.disk.level_1_threshold
                        {
                            println!("Start compact L1 -> L2 ...");
                            self_clone.compact(
                                CompactionLevel::L12,
                                level_1_size_threshold,
                                oldest_timestamp,
                            );

                            let level_2_size_threshold = level_1_size_threshold
                                * self_clone.config.disk.block_size_multiplier;

                            // level 2
                            if self_clone.level_2.read().unwrap().sst_readers.len()
                                > self_clone.config.disk.level_2_threshold
                            {
                                println!("Start compact L2 -> L3 ...");
                                self_clone.compact(
                                    CompactionLevel::L23,
                                    level_2_size_threshold,
                                    oldest_timestamp,
                                );
                            }
                        }
                    }
                }
            }));
        } else {
            println!("Auto compact is disabled");
        }
    }
}

impl LsmDisk {
    pub fn get_by_time(&self, key: &[u8], timestamp: u64) -> Option<Record> {
        // Use .rev() to search from newest to oldest
        for sst in self.level_0.read().unwrap().sst_readers.iter().rev() {
            let val = sst.get_by_time(key, timestamp);
            if val.is_some() {
                return val;
            }
        }

        // Use .rev() to search from newest to oldest
        for sst in self.level_1.read().unwrap().sst_readers.iter().rev() {
            let val = sst.get_by_time(key, timestamp);
            if val.is_some() {
                return val;
            }
        }

        // Use .rev() to search from newest to oldest
        for sst in self.level_2.read().unwrap().sst_readers.iter().rev() {
            let val = sst.get_by_time(key, timestamp);
            if val.is_some() {
                return val;
            }
        }

        // Use .rev() to search from newest to oldest
        for sst in self.level_3.read().unwrap().sst_readers.iter().rev() {
            let val = sst.get_by_time(key, timestamp);
            if val.is_some() {
                return val;
            }
        }

        None
    }

    pub fn get(&self, key: &[u8]) -> Option<Record> {
        self.get_by_time(key, u64::MAX)
    }
}

impl LsmDisk {
    pub(crate) fn update_manifest(&self) {
        // build manifest
        let manifest = Manifest {
            level_0: LevelManifest::from_level(&self.level_0),
            level_1: LevelManifest::from_level(&self.level_1),
            level_2: LevelManifest::from_level(&self.level_2),
            level_3: LevelManifest::from_level(&self.level_3),
        };

        // write manifest to file
        let manifest_path = format!("{}/manifest.toml", self.config.dir);
        let manifest_str = toml::to_string(&manifest).unwrap();
        std::fs::write(manifest_path, manifest_str).unwrap();
    }

    pub(crate) fn compact(
        self: &Arc<Self>,
        level: CompactionLevel,
        threshold: usize,
        oldest_timestamp: u64,
    ) {
        let mut approx_size = 0;
        // each outdated key contain at MOST 1 version
        let mut keys_outdated: HashSet<Vec<u8>> = HashSet::new();
        let mut map: SkipMap<Key, Record> = SkipMap::new();

        {
            // Determine compaction Level
            let (mut from, mut to) = level.from_to_levels(self);
            /*

            let (mut from, mut to) = match level {
                CompactionLevel::L01 => {
                    let from = self.level_0.write().unwrap();
                    let to = self.level_1.write().unwrap();
                    (from, to)
                }
                CompactionLevel::L12 => {
                    let from = self.level_1.write().unwrap();
                    let to = self.level_2.write().unwrap();
                    (from, to)
                }
                CompactionLevel::L23 => {
                    let from = self.level_2.write().unwrap();
                    let to = self.level_3.write().unwrap();
                    (from, to)
                }
            };
             */

            // Iterate files from latest to oldest:
            // 1. If the key does not exist in the map, insert key-record_size pair into the keys map;
            // 2. update the approximate size;
            // 3. if the approx size reaches the threshold, flush the btreemap content into a sst. clear the btreemap.
            let files: Vec<_> = from
                .sst_readers
                .iter()
                // iterate from newest to oldest
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

            // iterate from newest to oldest
            for (reader, size) in readers.iter_mut().zip(sizes) {
                let mut cursor = 0;
                while cursor < size {
                    // 1. read a record ------------------------------------
                    let kv = read_kv(reader);
                    let key = Key::from_slice(&kv.key, kv.timestamp);
                    cursor += kv.size();

                    // 2. Insert logic -------------------------------------
                    if key.timestamp < oldest_timestamp {
                        if keys_outdated.contains(&kv.key) {
                            // <1> Outdated && has newer version, skip older version
                        } else {
                            // <2> Outdated but no newer version, insert
                            approx_size += kv.size();
                            map.insert(key.clone(), kv.value);
                            keys_outdated.insert(kv.key);
                        }
                    } else {
                        // <3> Insert kv pair
                        approx_size += kv.size();
                        map.insert(key.clone(), kv.value);
                    }

                    if approx_size > threshold {
                        // build sst writer
                        let relpath = to.get_filename();

                        let writer = SstWriter::new(
                            self.config.sst.clone(),
                            self.config.dir.clone(),
                            relpath.clone(),
                            Memtable { map }.into(),
                        );
                        writer.build();

                        to.sst_readers.push(SstReader {
                            dir: self.config.dir.clone(),
                            filename: relpath,
                        });

                        map = SkipMap::new();
                        approx_size = 0;
                    }
                }
            }

            // write tail to a new sst
            if map.len() > 0 {
                let relpath = to.get_filename();

                let writer = SstWriter::new(
                    self.config.sst.clone(),
                    self.config.dir.clone(),
                    relpath.clone(),
                    Memtable { map }.into(),
                );
                writer.build();

                to.sst_readers.push(SstReader {
                    dir: self.config.dir.clone(),
                    filename: relpath,
                });
            }

            // dbg!(from.sst_readers.len());
            // dbg!(to.sst_readers.len());

            from.sst_readers.clear();
        } // `from` and `to` will be released here

        // `from` and `to` will be acquired here, thus deadlocks.
        // so we need to release them before updating manifest.
        self.update_manifest();
    }
}

#[derive(Debug)]
pub struct Kv {
    key: Vec<u8>,
    timestamp: u64,
    value: Record,
}

impl Kv {
    pub fn size(&self) -> usize {
        match &self.value {
            Record::Value(value) => 1 + 8 + 2 + 2 + self.key.len() + value.len(),
            Record::Tomb => 1 + 8 + 2 + self.key.len(),
        }
    }
}

fn read_kv(reader: &mut BufReader<&File>) -> Kv {
    // Read record type
    let mut buf = Vec::new();
    buf.resize(1, 0);

    let mut timestamp_buf = [0u8; 8];
    let mut size_buf = [0u8; 2];

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
            // Read timestamp
            reader.read_exact(&mut timestamp_buf).unwrap();
            let timestamp = u64::from_le_bytes(timestamp_buf.to_vec().try_into().unwrap());
            // println!("Value record: timestamp = {}", timestamp);

            // Value record
            // Read key size
            reader.read_exact(&mut size_buf).unwrap();
            let key_size = u16::from_le_bytes(size_buf.to_vec().try_into().unwrap()) as usize;
            // println!("Value record: key_size = {}", key_size);

            // Read value size
            reader.read_exact(&mut size_buf).unwrap();
            let value_size = u16::from_le_bytes(size_buf.to_vec().try_into().unwrap()) as usize;
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
                timestamp,
                value: Record::Value(Bytes::copy_from_slice(&value)),
            }
        }
        1 => {
            // Read timestamp
            reader.read_exact(&mut timestamp_buf).unwrap();
            let timestamp = u64::from_le_bytes(timestamp_buf.to_vec().try_into().unwrap());
            // println!("Tomb record: timestamp = {}", timestamp);

            // Tomb record
            // Read key size
            reader.read_exact(&mut size_buf).unwrap();
            let key_size = u16::from_le_bytes(size_buf.to_vec().try_into().unwrap()) as usize;
            // println!("Tomb record: key_size = {}", key_size);

            // Read key
            let mut key = Vec::new();
            buf.resize(key_size, 0);
            reader.read_exact(&mut buf).unwrap();
            key = buf;
            // println!("Read key: {:?}", String::from_utf8_lossy(&key));

            Kv {
                key,
                timestamp,
                value: Record::Tomb,
            }
        }
        type_id => panic!("Invalid record type: type_id = {}", type_id),
    }
}
