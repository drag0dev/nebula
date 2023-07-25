use std::{cell::RefCell, rc::Rc};
use super::{
    MemtableEntry, StorageCRUD
};

pub struct Memtable <S>
where
    S: StorageCRUD
{
    storage: S,

    /// amount of data in the memtable in bytes
    pub len: u64,

    /// max amount of data to be placed inside memtable in bytes
    pub capacity: u64,
}

impl<S> Memtable<S>
where
    S: StorageCRUD
{ pub fn new(storage: S, capacity: u64) -> Self {
        Memtable{
            storage,
            len: 0,
            capacity
        }
    }

    pub fn create(&mut self, entry: MemtableEntry) {
        self.storage.create(entry);
    }

    pub fn read(&mut self, key: String) -> Option<Rc<RefCell<MemtableEntry>>> {
        self.storage.read(key)
    }

    pub fn update(&mut self, entry: MemtableEntry) {
        self.storage.update(entry);
    }

    pub fn delete(&mut self, entry: MemtableEntry) {
        self.storage.delete(entry);
    }

    pub fn prefix_scan(&mut self, prefix: String) -> Vec<Rc<RefCell<MemtableEntry>>> {
        let mut res = Vec::new();

        for entry in self.storage.entries() {
            let borrowed_entry = entry.borrow();
            if borrowed_entry.key.starts_with(&prefix) && borrowed_entry.value.is_some() {
                res.push(Rc::clone(&entry));
            }
        }

        res
    }
}
