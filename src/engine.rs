use crate::building_blocks::{
    BTree, Cache, Entry, LSMTree, Memtable, MemtableEntry, WriteAheadLog, SF,
};
use crate::repl::Commands;
use crate::repl::REPL;
use anyhow::{Context, Result};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Engine {
    memtable: Memtable<BTree<String, Rc<RefCell<MemtableEntry>>>>,
    cache: Cache,
    wal: WriteAheadLog,
    lsm: LSMTree<SF>,
}

impl Engine {
    pub fn new() -> Result<Engine> {
        let b_tree: BTree<String, Rc<RefCell<MemtableEntry>>> = BTree::new();

        let memtable = Memtable::new(
            b_tree,
            2,
            crate::building_blocks::FileOrganization::SingleFile(()),
            0.1,
            50,
            String::from("data/table_data"),
        );

        let cache = Cache::new(400);

        let wal = WriteAheadLog::new("data/WAL", 2000).context("creating WAL")?;

        let lsm = LSMTree::<SF>::new(0.1, 100, String::from("data/table_data"), 3, 3);


        Ok(Engine {
            memtable,
            cache,
            wal,
            lsm,
        })
    }

    pub fn start(&mut self) -> Result<()> {
        // check for existing data and integrity
        //		 -> init the structs
        //		-> call repl REPLnew getquery
        //		  -> match the repl commands -> do something writepath or readpath
        //		-> put and delete (the mutable things go to WAL then memtable)
        //		-> list range?
        //		  -> bf command?
        //		  -> quit() (flush memtable and purge WAL) then u can quit gracefully
        //		-> bubble any errors up to main() and then just print it
        //		  -> printError()
        //	delete delet all local files (rename test-data -> data)
        //	init not required? ---
        //
        //

        // check existing data and integrity

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
                        println!("QUIT");
                        break;
                    }
                    _ => { }
                }
            } else {
                query.context("getting query")?;
            }
        }
        Ok(())
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
        self.memtable.delete(entry);
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
