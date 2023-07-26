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
{
    pub fn new(storage: S, capacity: u64) -> Self {
        Memtable{
            storage,
            len: 0,
            capacity
        }
    }

    pub fn create(&mut self, entry: MemtableEntry) {
        self.update_len(&entry);
        self.storage.create(entry);

        if self.len >= self.capacity {
            // TODO: flush
            unimplemented!();
        }
    }

    pub fn read(&mut self, key: String) -> Option<Rc<RefCell<MemtableEntry>>> {
        self.storage.read(key)
    }

    /// len is calculated as if all updates create a new entry, which is not always the case
    pub fn update(&mut self, entry: MemtableEntry) {
        self.update_len(&entry);
        self.storage.update(entry);

        if self.len >= self.capacity {
            // TODO: flush
            unimplemented!();
        }
    }

    /// len is calculated as if all deletes create a new entry, which is not always the case
    pub fn delete(&mut self, entry: MemtableEntry) {
        self.update_len(&entry);
        self.storage.delete(entry);

        if self.len >= self.capacity {
            // TODO: flush
            unimplemented!();
        }
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

    pub fn range_scan(&mut self, start: String, end: String) -> Vec<Rc<RefCell<MemtableEntry>>> {
        let mut res = Vec::new();

        for entry in self.storage.entries() {
            let borrowed_entry = entry.borrow();
            if (borrowed_entry.key >= start && borrowed_entry.key <= end) && borrowed_entry.value.is_some() {
                res.push(Rc::clone(&entry));
            }
        }

        res
    }

    fn update_len(&mut self, entry: &MemtableEntry) {
        let value_len = if let Some(val) = entry.value.as_ref() { val.len() as u64 } else { 0 };

        // 16 byes for timestamp
        let new_size = 16 + entry.key.len() as u64 + value_len;

        self.len += new_size;
    }
}
