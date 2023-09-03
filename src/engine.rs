use crate::building_blocks::{
    BTree, Cache, Entry, LSMTree, LSMTreeInterface, Memtable, MemtableEntry, WriteAheadLog,
    WriteAheadLogReader, SF,
};
use crate::repl::Commands;
use crate::repl::REPL;
use anyhow::{Context, Result};
use std::cell::RefCell;
use std::mem;
use std::process::exit;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

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

                let value;

                // Entry path
                if entry.value.is_some() {
                    let strval = String::from_utf8(entry.value.unwrap())
                        .context("converting val to String")?;

                    value = Some(strval);

                    let mementry = MemtableEntry {
                        timestamp: entry.timestamp,
                        key: key.clone(),
                        value,
                    };

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
                        self.put(key, Some(value))
                            .context("putting {key} {value}")?;
                    }
                    Commands::Delete { key } => {
                        self.delete(key.clone()).context("deleting {key}")?;
                        println!("DELETE: {}", key);
                    }
                    Commands::Quit => {
                        println!("quitting...");
                        self.quit().context("quitting")?;
                        // flush memtable
                        // purge wal
                        // exit
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

        self.wal.purge().context("purging wal")?;

        exit(0);
    }

    fn get(&mut self, key: Vec<u8>) -> Result<()> {
        let strkey = String::from_utf8(key.clone()).context("converting key to string")?;
        let result = self.memtable.read(strkey.clone());
        if let Some(mem_entry) = result {
            println!("ENTRY: {:?}", mem_entry);
            return Ok(());
        }

        if let Some(entry) = self.cache.find(&key[..]) {
            println!("ENTRY: {:?}", entry);
            return Ok(());
        }

        let result: Option<Entry> = self.lsm.get(key);
        if let Some(entry) = result {
            self.cache.add(&entry.key, entry.value.clone().as_deref());
            println!("ENTRY: {:?}", entry);
        } else {
            println!("KEY NOT FOUND");
        }

        Ok(())
    }

    fn put(&mut self, key: String, value: Option<String>) -> Result<()> {
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
        let entry = MemtableEntry::new(get_timestamp()?, key, None);
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
