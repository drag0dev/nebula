use crate::building_blocks::{
    BTree, Cache, Entry, LSMTree, LSMTreeInterface, Memtable, MemtableEntry, WriteAheadLog,
    WriteAheadLogReader, SF, BloomFilter, HyperLogLog, CountMinSketch, SimHash, SkipList, TokenBucket, MF, BINCODE_OPTIONS, similarity,
};
use crate::building_blocks::FileOrganization::{SingleFile, MultiFile};
use crate::repl::REPL;
use crate::repl::{BloomFilterCommands, CMSCommands, Commands, HLLCommands, SimHashCommands};
use crate::utils::config::{Config, MemtableStorage};
use anyhow::{Context, Result};
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};
use bincode::Options;

pub struct Engine {
    memtable: Memtable,
    cache: Cache,
    wal: WriteAheadLog,
    lsm: Box<dyn LSMTreeInterface>,
    config: Config,
    token_bucket: TokenBucket,
}

impl Engine {
    pub fn lsm_init() {

    }
    pub fn new() -> Result<Self> {
        let config;
        if let Ok(ok_config) = Config::load_from_file().context("failed to load config, defaulting") {
            config = ok_config;
        } else {
            config = Config::default();
        }

        let memtable_vars = config.memtable.get_values();
        let mut memtable;
        match memtable_vars.0 {
            MemtableStorage::BTree => {
                let storage: BTree<String, Rc<RefCell<MemtableEntry>>> = BTree::new();

                memtable = Memtable::new(
                    Box::new(storage),
                    memtable_vars.1,
                    memtable_vars.2,
                    memtable_vars.3,
                    memtable_vars.4,
                    memtable_vars.5,
                );
            }
            MemtableStorage::SkipList => {
                let storage: SkipList<MemtableEntry> = SkipList::new(config.skiplist.get_values());

                memtable = Memtable::new(
                    Box::new(storage),
                    memtable_vars.1,
                    memtable_vars.2,
                    memtable_vars.3,
                    memtable_vars.4,
                    memtable_vars.5,
                );
            }
        }

        let cache = Cache::new(config.cache.get_values());
        let wal_vars = config.wal.get_values();
        let wal = WriteAheadLog::new(&wal_vars.0, wal_vars.1).context("creating WAL")?;
        let lsm_vars = config.lsm.get_values();
        let token_bucket_vars = config.token_bucket.get_values();
        let token_bucket = TokenBucket::new(token_bucket_vars.0, token_bucket_vars.1);
        let mut engine = match lsm_vars.0 {
            SingleFile(()) => {
                let lsm = LSMTree::<SF>::new(lsm_vars.1, lsm_vars.2, lsm_vars.3.clone(), lsm_vars.4, lsm_vars.5);
                Engine {
                    memtable,
                    cache,
                    wal,
                    lsm: Box::new(lsm),
                    config,
                    token_bucket
                }
            },
            MultiFile(()) => {
                let lsm = LSMTree::<MF>::new(lsm_vars.1, lsm_vars.2, lsm_vars.3.clone(), lsm_vars.4, lsm_vars.5);
                Engine {
                    memtable,
                    cache,
                    wal,
                    lsm: Box::new(lsm),
                    config,
                    token_bucket
                }
            }
        };

        // load data if found
        engine.lsm.load().context("loading data into lsm")?;

        let mut wal_reader =
            WriteAheadLogReader::iter(&wal_vars.0).context("getting wal_reader iter")?;

        // if wal has entries, rebuild memtable and purge wal
        while let Some(vec_entries) = wal_reader.next() {
            let mut entries = vec_entries.context("unrwrapping entries")?;

            while let Some(entry) = entries.pop() {
                let timestamp = entry.timestamp;
                let key = String::from_utf8(entry.key).context("converting key to String")?;

                // Entry path
                if entry.value.is_some() {
                    let mementry = MemtableEntry::new(entry.timestamp, key.clone(), entry.value);

                    engine.memtable.create(mementry);
                    continue;
                }

                // tombstone path
                let tombstone = MemtableEntry {
                    timestamp,
                    key,
                    value: None,
                };

                if let Some(result) = engine.memtable.delete(tombstone) {
                    if let Ok(_) = result {
                        engine.lsm.insert("memtable")
                            .context("inserting memetable into lsm")?;
                    }
                }
            }
        }
        engine.wal.purge().context("purging wal")?;

        Ok(engine)
    }

    pub fn start(&mut self) -> Result<()> {
        let mut repl = REPL::new();
        loop {
            let query = repl.get_query();
            if let Ok(query) = query {
                if self.token_bucket.take(1) {
                    match query.commands {
                        Commands::Get { key } => {
                            let vec_key = key.as_bytes().to_vec();
                            self.get(vec_key).context("getting entry from lsm")?;
                        }
                        Commands::Put { key, value } => {
                            self.put(key, Some(value.as_bytes().to_vec()))
                                .context("putting {key} {value}")?;
                        }
                        Commands::Delete { key } => {
                            self.delete(key.clone()).context("deleting {key}")?;
                        }
                        Commands::Bf(cmd) => self.bloomfilter(cmd)?,
                        Commands::Hll(cmd) => self.hll(cmd)?,
                        Commands::Cms(cmd) => self.cms(cmd)?,
                        Commands::Sh(cmd) => self.simhash(cmd)?,
                        Commands::Quit => {
                            self.quit().context("quitting")?;
                            break;
                        }

                        Commands::List { key_prefix, pagination } => {
                            let mut mem_res = self.memtable.prefix_scan(key_prefix.clone())
                                .iter()
                                .map(|e| Entry::from(&*e.as_ref().borrow()))
                                .collect::<Vec<_>>();
                            let lsm_res = self.lsm.prefix_scan(&key_prefix).context("running prefix scan")?;
                            mem_res.extend_from_slice(&lsm_res);
                            if pagination.is_none() {
                                for entry in mem_res {
                                    print_entry(&entry)?;
                                    println!();
                                }
                            } else {
                                let pagination = pagination.unwrap();
                                let mut page = pagination[0];
                                if page == 0 {page = 1};
                                let page_size = pagination[1];
                                let iter = mem_res
                                    .iter()
                                    .step_by(page_size as usize)
                                    .skip((page-1) as usize);
                                let mut counter = 0;
                                for entry in iter {
                                    if counter == page_size { break; }
                                    print_entry(&entry)?;
                                    println!();
                                    counter += 1;
                                }
                            }
                        }

                        Commands::RangeScan { start_key, end_key, pagination } => {
                            let mut mem_res = self.memtable.range_scan(start_key.clone(), end_key.clone())
                                .iter()
                                .map(|e| Entry::from(&*e.as_ref().borrow()))
                                .collect::<Vec<_>>();
                            let lsm_res = self.lsm.range_scan(&start_key, &end_key).context("running prefix scan")?;
                            mem_res.extend_from_slice(&lsm_res);
                            if pagination.is_none() {
                                for entry in mem_res {
                                    print_entry(&entry)?;
                                    println!();
                                }
                            } else {
                                let pagination = pagination.unwrap();
                                let mut page = pagination[0];
                                if page == 0 {page = 1};
                                let page_size = pagination[1];
                                let iter = mem_res
                                    .iter()
                                    .step_by(page_size as usize)
                                    .skip((page-1) as usize);
                                let mut counter = 0;
                                for entry in iter {
                                    if counter == page_size { break; }
                                    print_entry(&entry)?;
                                    println!();
                                    counter += 1;
                                }
                            }
                        }
                    }
                } else {
                    let timestamp = chrono::Local::now();
                    println!("{} => FAIL...[TOKEN BUCKET CONSTRAINT]", timestamp.format("%H:%M:%S")); 
                }
            } else {
                query.context("getting query")?;
            }
        }
        Ok(())
    }

    fn quit(&mut self) -> Result<()> {
        if self.memtable.len > 0 {
            self.memtable.flush().context("flushing memtable")?;
            self.handle_memtable_flush()
                .context("handling memtable flush")?;
        }

        self.wal.purge().context("purging wal")
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Entry>> {
        let strkey = String::from_utf8(key.clone()).context("converting key to string")?;
        let result = self.memtable.read(strkey.clone());
        if let Some(mem_entry) = result {
            if !is_key_reserved(&mem_entry.borrow().key.as_bytes()) {
                print_memtable_entry(&*mem_entry.as_ref().borrow())?;
            }
            return Ok(Some(Entry::from(&*mem_entry.as_ref().borrow())));
        }

        if let Some(entry) = self.cache.find(&key[..]) {
            if entry.is_some() {
                if !is_key_reserved(&key) {
                    println!("Key: {strkey}");
                    let value = String::from_utf8(entry.as_ref().unwrap().to_vec())
                        .context("converting key to string")?;
                    println!("Value: {value}");
                }
            }
            let entry = Entry {
                timestamp: 0,
                key,
                value: entry,
            };
            return Ok(Some(entry));
        }

        let result: Option<Entry> = self.lsm.get(key);
        if let Some(entry) = result {
            self.cache.add(&entry.key, entry.value.clone().as_deref());
            if !is_key_reserved(&entry.key) {
                print_entry(&entry)?;
            }
            return Ok(Some(entry));
        } else {
            println!("Key not found");
        }

        Ok(None)
    }

    fn put(&mut self, key: String, value: Option<Vec<u8>>) -> Result<()> {
        let mementry = MemtableEntry::new(get_timestamp()?, key, value);
        let walentry = Entry::from(&mementry);
        self.wal.add(&walentry).context("adding to WAL")?;

        // I don't know why it only works this way
        if let Some(result) = self.memtable.create(mementry) {
            if let Ok(_) = result {
                self.handle_memtable_flush()
            } else {
                result
            }
        } else {
            Ok(())
        }
    }

    fn delete(&mut self, key: String) -> Result<()> {
        let entry = MemtableEntry::new(get_timestamp()?, key, None);
        let walentry = Entry::from(&entry);
        self.wal.add(&walentry).context("adding to WAL")?;
        if let Some(result) = self.memtable.delete(entry) {
            if let Ok(_) = result {
                return self.handle_memtable_flush();
            } else {
                return result;
            }
        }
        Ok(())
    }

    fn bloomfilter(&mut self, cmd: BloomFilterCommands) -> Result<()> {
        match cmd {
            BloomFilterCommands::Add {
                bloom_filter_key,
                value,
            } => {
                if !key_starts_with(&bloom_filter_key, "bf_") {
                    return Ok(());
                }
                let bf_ser = self.get(bloom_filter_key.clone().into_bytes())?;
                if let Some(bf_ser) = bf_ser {
                    if let Some(bf_value) = bf_ser.value {
                        let mut bf = BloomFilter::deserialize(&bf_value[..])?;
                        bf.add(&value.as_bytes()[..])
                            .context("adding value to the bloomfilter")?;
                        let bf_ser = bf.serialize()?;
                        self.put(bloom_filter_key, Some(bf_ser))?;
                    } else {
                        println!("Entry not found");
                    }
                }
            }
            BloomFilterCommands::New { bloom_filter_key } => {
                if !key_starts_with(&bloom_filter_key, "bf_") {
                    return Ok(());
                }
                let bf_vars = self.config.bf.get_values();
                let bf = BloomFilter::new(bf_vars.0, bf_vars.1);
                let bf_ser = bf.serialize()?;
                self.put(bloom_filter_key, Some(bf_ser))?;
            }
            BloomFilterCommands::Check {
                bloom_filter_key,
                value,
            } => {
                if !key_starts_with(&bloom_filter_key, "bf_") {
                    return Ok(());
                }
                let bf_ser = self.get(bloom_filter_key.clone().into_bytes())?;
                if let Some(bf_ser) = bf_ser {
                    if let Some(bf_value) = bf_ser.value {
                        let bf = BloomFilter::deserialize(&bf_value[..])?;
                        let found = bf
                            .check(value.as_bytes())
                            .context("checkign if the value is present in the bf")?;
                        if found {
                            println!("Value is present in the bloomfilter");
                        } else {
                            println!("Value is not present in the bloomfilter");
                        }
                    } else {
                        println!("Entry not found");
                    }
                }
            }
        }
        Ok(())
    }

    fn hll(&mut self, cmd: HLLCommands) -> Result<()> {
        match cmd {
            HLLCommands::New { hll_key } => {
                if !key_starts_with(&hll_key, "hll_") {
                    return Ok(());
                }
                let hll_vars = self.config.hll.get_values();
                let hll = HyperLogLog::new(hll_vars);
                let hll_ser = hll.serialize()?;
                self.put(hll_key, Some(hll_ser))?;
            }
            HLLCommands::Add { hll_key, value } => {
                if !key_starts_with(&hll_key, "hll_") {
                    return Ok(());
                }
                let hll = self.get(hll_key.clone().into_bytes())?;
                if let Some(hll) = hll {
                    if let Some(hll_ser) = hll.value {
                        let mut hll = HyperLogLog::deserialize(&hll_ser[..])?;
                        hll.add(value.as_bytes());
                        let hll_ser = hll.serialize()?;
                        self.put(hll_key, Some(hll_ser))?;
                    } else {
                        println!("Entry not found");
                    }
                }
            }
            HLLCommands::Count { hll_key } => {
                if !key_starts_with(&hll_key, "hll_") {
                    return Ok(());
                }
                let hll = self.get(hll_key.clone().into_bytes())?;
                if let Some(hll) = hll {
                    if let Some(hll_ser) = hll.value {
                        let hll = HyperLogLog::deserialize(&hll_ser[..])?;
                        println!("Count: {}", hll.count());
                    } else {
                        println!("Entry not found");
                    }
                }
            }
        }
        Ok(())
    }

    fn cms(&mut self, cmd: CMSCommands) -> Result<()> {
        match cmd {
            CMSCommands::New { cms_key } => {
                if !key_starts_with(&cms_key, "cms_") {
                    return Ok(());
                }
                let cms_vars = self.config.cms.get_values();
                let cms = CountMinSketch::new(cms_vars.0, cms_vars.1);
                let cms_ser = cms.serialize()?;
                self.put(cms_key, Some(cms_ser))?;
            }
            CMSCommands::Count { cms_key } => {
                if !key_starts_with(&cms_key, "cms_") {
                    return Ok(());
                }
                let cms_ser = self.get(cms_key.clone().into_bytes())?;
                if let Some(cms_ser) = cms_ser {
                    if let Some(cms_ser) = cms_ser.value {
                        let cms = CountMinSketch::deserialize(&cms_ser)?;
                        println!("Count: {}", cms.count("").context("counting in cms")?);
                    } else {
                        println!("Entry not found");
                    }
                }
            }
            CMSCommands::Add { cms_key, value } => {
                if !key_starts_with(&cms_key, "cms_") {
                    return Ok(());
                }
                let cms_ser = self.get(cms_key.clone().into_bytes())?;
                if let Some(cms_ser) = cms_ser {
                    if let Some(cms_ser) = cms_ser.value {
                        let mut cms = CountMinSketch::deserialize(&cms_ser)?;
                        cms.add(&value)?;
                        let cms_ser = cms.serialize()?;
                        self.put(cms_key, Some(cms_ser))?;
                        println!("Count: {}", cms.count("").context("counting in cms")?);
                    } else {
                        println!("Entry not found");
                    }
                }
            }
        }
        Ok(())
    }

    fn simhash(&mut self, cmd: SimHashCommands) -> Result<()> {
        match cmd {
            SimHashCommands::Hash { key, value } => {
                if !key_starts_with(&key, "sh_") { return Ok(()) }
                let stopwortds = HashSet::from(["this".to_owned()]);
                let sh_vars = self.config.simhash.get_values();
                let mut sh = SimHash::new(sh_vars.0, sh_vars.1);
                sh.calculate(&value);

                let fingerprint = sh.fingerprint();
                let fingerpint_ser = BINCODE_OPTIONS.serialize(&fingerprint)?;
                self.put(key, Some(fingerpint_ser))?;
            },
            SimHashCommands::Similarity { left_key, right_key } => {
                if !key_starts_with(&left_key, "sh_") { return Ok(()) }
                if !key_starts_with(&right_key, "sh_") { return Ok(()) }
                let left_footprint;
                let left = self.get(left_key.clone().into_bytes())?;
                if let Some(left) = left {
                    if let Some(left_ser) = left.value {
                        left_footprint = BINCODE_OPTIONS.deserialize(&left_ser[..])?;
                    } else {
                        println!("Key {} not found", left_key);
                        return Ok(());
                    }
                } else {return Ok(())}

                let right_footprint;
                let right = self.get(right_key.clone().into_bytes())?;
                if let Some(right) = right {
                    if let Some(right_ser) = right.value {
                        right_footprint = BINCODE_OPTIONS.deserialize(&right_ser[..])?;
                    } else {
                        println!("Key {} not found", right_key);
                        return Ok(());
                    }
                } else {return Ok(())}

                println!("Similarity: {}", similarity(left_footprint, right_footprint));
            }
        }
        Ok(())
    }

    fn handle_memtable_flush(&mut self) -> Result<()> {
        self.lsm
            .insert("memtable")
            .context("inserting memetable into lsm")
    }
}

fn get_timestamp() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("getting epoch time")?
        .as_nanos())
}

fn print_entry(entry: &Entry) -> Result<()> {
    if entry.value.is_none() {
        println!("Key not found");
    } else {
        let key = String::from_utf8(entry.key.clone()).context("converting key to string")?;
        println!("Key: {}", key);
        print_value(&entry.value.as_ref().unwrap())?;
    }
    Ok(())
}

fn print_memtable_entry(entry: &MemtableEntry) -> Result<()> {
    if entry.value.is_none() {
        println!("Key not found");
    } else {
        println!("Key: {}", entry.key);
        print_value(&entry.value.as_ref().unwrap())?;
    }
    Ok(())
}

fn print_value(input: &[u8]) -> Result<()> {
    let value = String::from_utf8(input.to_vec()).context("converting value to string")?;
    println!("Value: {value}");
    Ok(())
}

fn is_key_reserved(input: &[u8]) -> bool {
    let mut reserved = false;
    for prefix in ["bf_", "cms_", "hll_", "sh_"] {
        if input.starts_with(prefix.as_bytes()) {
            reserved = true;
        }
    }
    reserved
}

fn key_starts_with(key: &str, prefix: &str) -> bool {
    let key_vec = key.as_bytes();
    if !key_vec.starts_with(prefix.as_bytes()) {
        println!(
            "key {} doesnt start with the reserved prefix {}",
            key, prefix
        );
        return false;
    }
    true
}
