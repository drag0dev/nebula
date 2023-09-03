use std::{cell::RefCell, rc::Rc};
use crate::{building_blocks::{FileOrganization, SSTableBuilderSingleFile, Entry, SSTableBuilderMultiFile}, utils::merge_sort::merge_sort};
use anyhow::{Context, Result};
use super::{
    MemtableEntry, StorageCRUD
};

/// memetable flushes to the disk only when the amount of data inside memtable
/// is greater or equal to capacity, in order to avoid checking multiple times for one action
/// create, update and delete return the path to the new sstable if flush happened
/// sstable created when flushing memtable is always called "memtable"
pub struct Memtable <S>
where
    S: StorageCRUD
{
    storage: S,

    /// number of entries in the memtable
    pub len: u64,

    /// max number of entries to be placed inside memtable
    pub capacity: u64,

    sstable_type: FileOrganization,
    fp_prob: f64,
    summary_nth: u64,
    data_folder: String,
}

impl<S> Memtable<S>
where
    S: StorageCRUD
{
    pub fn new(storage: S, capacity: u64, sstable_type: FileOrganization, fp_prob: f64, summary_nth: u64, data_folder: String) -> Self {
        Memtable{
            storage,
            len: 0,
            capacity,
            sstable_type,
            fp_prob,
            summary_nth,
            data_folder
        }
    }

    pub fn create(&mut self, entry: MemtableEntry) -> Option<Result<()>> {
        if self.storage.read(entry.key.clone()).is_none() {
            self.len += 1;
        }
        self.storage.create(entry);

        assert!(self.len <= self.capacity);
        if self.len == self.capacity {
            return Some(self.flush());
        }
        None
    }

    pub fn read(&mut self, key: String) -> Option<Rc<RefCell<MemtableEntry>>> {
        self.storage.read(key)
    }

    pub fn update(&mut self, entry: MemtableEntry) -> Option<Result<()>> {
        if self.storage.read(entry.key.clone()).is_none() {
            self.len += 1;
        }
        self.storage.update(entry);

        assert!(self.len <= self.capacity);
        if self.len == self.capacity {
            return Some(self.flush());
        }
        None
    }

    pub fn delete(&mut self, entry: MemtableEntry) -> Option<Result<()>> {
        if self.storage.read(entry.key.clone()).is_none() {
            self.len += 1;
        }
        self.storage.delete(entry);

        assert!(self.len <= self.capacity);
        if self.len == self.capacity {
            return Some(self.flush());
        }
        None
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
            if borrowed_entry.key >= start && borrowed_entry.key <= end {
                res.push(Rc::clone(&entry));
            }
        }

        res
    }

    fn flush(&mut self) -> Result<()> {
        let wrapped_entries = self.storage.entries();
        let ref_entries = wrapped_entries
            .iter()
            .map(|e| e.as_ref().borrow()).collect::<Vec<_>>();
        let mut entries = ref_entries
            .iter()
            .map(|e| &**e).collect::<Vec<_>>();
        merge_sort(&mut entries);

        if self.sstable_type == FileOrganization::SingleFile(()) {
            let mut builder = SSTableBuilderSingleFile::new(
                &self.data_folder,
                "memtable", entries.len() as u64,
                self.fp_prob, self.summary_nth)
                .context("creating single file builder")?;

            for entry in entries {
                let entry = Entry::from(entry);
                builder.insert(entry)
                    .context("inserting entry")?;
            }
            builder.finish_data()
                .context("finishing singlefile builder")?;
        } else {
            let mut builder = SSTableBuilderMultiFile::new(
                &self.data_folder,
                "memtable", entries.len() as u64,
                self.fp_prob, self.summary_nth)
                .context("creating multifile builder")?;

            for entry in entries {
                let entry = Entry::from(entry);
                builder.insert(entry)
                    .context("inserting entry")?;
            }
            builder.finish()
                .context("finishing multifile builder")?;
        }

        self.len = 0;
        self.storage.clear();
        Ok(())
    }
}
