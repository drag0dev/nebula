use crate::building_blocks::{
    BTree, Cache, Entry, LSMTree, LSMTreeInterface, Memtable, MemtableEntry, WriteAheadLog,
    WriteAheadLogReader, SF, BloomFilter, HyperLogLog, CountMinSketch, SimHash,
};
use crate::repl::{Commands, BloomFilterCommands, HLLCommands, CMSCommands, SimHashCommands};
use crate::repl::REPL;
use anyhow::{Context, Result};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

// TODO: add config as a field to the engine
// TODO: cms input value when counting?

pub struct Engine {
    memtable: Memtable,
    cache: Cache,
    wal: WriteAheadLog,
    lsm: Box<dyn LSMTreeInterface>,
}

impl Engine {
    pub fn new() -> Result<Self> {
        let b_tree: BTree<String, Rc<RefCell<MemtableEntry>>> = BTree::new();

        let mut memtable = Memtable::new(
            Box::new(b_tree),
            2,
            crate::building_blocks::FileOrganization::SingleFile(()),
            0.1,
            50,
            String::from("data/table_data"),
        );

        let mut lsm = LSMTree::<SF>::new(0.1, 100, String::from("data/table_data"), 3, 3);

        // load data if found
        lsm.load().context("loading data into lsm")?;

        let mut wal = WriteAheadLog::new("data/WAL", 2000).context("creating WAL")?;

        let mut wal_reader =
            WriteAheadLogReader::iter("data/WAL").context("getting wal_reader iter")?;

        // if wal has entries, rebuild memtable and purge wal
        while let Some(vec_entries) = wal_reader.next() {
            let mut entries = vec_entries.context("unrwrapping entries")?;

            while let Some(entry) = entries.pop() {
                let timestamp = entry.timestamp;
                let key = String::from_utf8(entry.key).context("converting key to String")?;

                // Entry path
                if entry.value.is_some() {
                    let mementry = MemtableEntry::new( entry.timestamp, key.clone(), entry.value);

                    memtable.create(mementry);
                    continue;
                }

                // tombstone path
                let tombstone = MemtableEntry {
                    timestamp,
                    key,
                    value: None,
                };

                if let Some(result) = memtable.delete(tombstone) {
                    if let Ok(_) = result {
                        // println!("flushing");
                        lsm.insert("memtable")
                            .context("inserting memetable into lsm")?;
                    }
                }
            }
        }
        wal.purge().context("purging wal")?;

        let cache = Cache::new(400);

        Ok(Engine {
            memtable,
            cache,
            wal,
            lsm: Box::new(lsm),
        })
    }

    pub fn start(&mut self) -> Result<()> {
        let mut repl = REPL::new();
        loop {
            let query = repl.get_query();
            if let Ok(query) = query {
                match query.commands {
                    Commands::Get { key } => {
                        let vec_key = key.as_bytes().to_vec();
                        self.get(vec_key).context("getting entry from lsm")?;
                    }
                    Commands::Put { key, value } => {
                        println!("PUT: {} {}", key, value);
                        self.put(key, Some(value.as_bytes().to_vec()))
                            .context("putting {key} {value}")?;
                    }
                    Commands::Delete { key } => {
                        self.delete(key.clone()).context("deleting {key}")?;
                        println!("DELETE: {}", key);
                    }
                    Commands::Bf(cmd) => self.bloomfilter(cmd)?,
                    Commands::Hll(cmd) => self.hll(cmd)?,
                    Commands::Cms(cmd) => self.cms(cmd)?,
                    Commands::Sh(cmd) => self.simhash(cmd)?,
                    Commands::Quit => {
                        println!("quitting...");
                        self.quit().context("quitting")?;
                        break;
                    }
                    _ => {}
                }
            } else {
                query.context("getting query")?;
            }
        }
        Ok(())
    }

    fn quit(&mut self) -> Result<()> {
        self.memtable.flush().context("flushing memtable")?;
        self.handle_memtable_flush().context("handling memtable flush")?;

        self.wal.purge().context("purging wal")
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Entry>> {
        let strkey = String::from_utf8(key.clone()).context("converting key to string")?;
        let result = self.memtable.read(strkey.clone());
        if let Some(mem_entry) = result {
            println!("ENTRY: {:?}", mem_entry);
            return Ok(Some(Entry::from(&*mem_entry.as_ref().borrow())));
        }

        if let Some(entry) = self.cache.find(&key[..]) {
            println!("ENTRY: {:?}", entry);
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
            println!("ENTRY: {:?}", entry);
            return Ok(Some(entry));
        } else {
            println!("KEY NOT FOUND");
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
                println!("OK PUT");
                return self.handle_memtable_flush();
            } else {
                return result;
            }
        }

        Ok(())
    }

    fn delete(&mut self, key: String) -> Result<()> {
        let entry = MemtableEntry::new_string(get_timestamp()?, key, None);
        let walentry = Entry::from(&entry);
        self.wal.add(&walentry).context("adding to WAL")?;
        if let Some(result) = self.memtable.delete(entry) {
            if let Ok(_) = result {
                println!("OK DELETE");
                return self.handle_memtable_flush();
            } else {
                return result;
            }
        }
        Ok(())
    }

    fn bloomfilter(&mut self, cmd: BloomFilterCommands) -> Result<()> {
        match cmd {
            BloomFilterCommands::Add { bloom_filter_key, value } => {
                let bf_ser = self.get(bloom_filter_key.clone().into_bytes())?;
                if let Some(bf_ser) = bf_ser {
                    if let Some(bf_value) = bf_ser.value {
                        let mut bf = BloomFilter::deserialize(&bf_value[..])?;
                        bf.add(&value.as_bytes()[..]).context("adding value to the bloomfilter")?;
                        let bf_ser = bf.serialize()?;
                        self.put(bloom_filter_key, Some(bf_ser))?;
                    } else {
                        println!("Entry not found");
                    }
                }
            },
            BloomFilterCommands::New { bloom_filter_key } => {
                let bf = BloomFilter::new(5, 0.01);
                let bf_ser = bf.serialize()?;
                self.put(bloom_filter_key, Some(bf_ser))?;
            },
            BloomFilterCommands::Check { bloom_filter_key, value } => {
                let bf_ser = self.get(bloom_filter_key.clone().into_bytes())?;
                if let Some(bf_ser) = bf_ser {
                    if let Some(bf_value) = bf_ser.value {
                        let bf = BloomFilter::deserialize(&bf_value[..])?;
                        let found = bf.check(value.as_bytes())
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
                let hll = HyperLogLog::new(8);
                let hll_ser = hll.serialize()?;
                self.put(hll_key, Some(hll_ser))?;
            }
            HLLCommands::Add { hll_key, value } => {
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
            },
            HLLCommands::Count { hll_key } => {
                let hll = self.get(hll_key.clone().into_bytes())?;
                if let Some(hll) = hll {
                    if let Some(hll_ser) = hll.value {
                        let hll = HyperLogLog::deserialize(&hll_ser[..])?;
                        println!("Count: {}", hll.count());
                    } else {
                        println!("Entry not found");
                    }
                }
            },
        }
        Ok(())
    }

    fn cms(&mut self, cmd: CMSCommands) -> Result<()> {
        match cmd {
            CMSCommands::New { cms_key } => {
                let cms = CountMinSketch::new(0.1, 0.1);
                let cms_ser = cms.serialize()?;
                self.put(cms_key, Some(cms_ser))?;
            },
            CMSCommands::Count { cms_key } => {
                let cms_ser = self.get(cms_key.clone().into_bytes())?;
                if let Some(cms_ser) = cms_ser {
                    if let Some(cms_ser) = cms_ser.value {
                        let cms = CountMinSketch::deserialize(&cms_ser)?;
                        println!("Count: {}", cms.count("").context("counting in cms")?);
                    } else {
                        println!("Entry not found");
                    }
                }
            },
            CMSCommands::Add { cms_key, value } => {
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
            },
        }
        Ok(())
    }

    fn simhash(&mut self, cmd: SimHashCommands) -> Result<()> {
        match cmd {
            SimHashCommands::Hash { value } => { },
            SimHashCommands::Similarity { left, right } => { }
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
