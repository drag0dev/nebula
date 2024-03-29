use std::{cell::RefCell, rc::Rc};
use super::MemtableEntry;

/// the underlying structure used for storing memtable_entry implements given trait
pub trait StorageCRUD {
    fn create(&mut self, item: MemtableEntry);

    fn read(&mut self, key: String) -> Option<Rc<RefCell<MemtableEntry>>>;

    /// updates an existing item, otherwise creates a new one
    fn update(&mut self, item: MemtableEntry);

    /// sets value field to None
    /// if it doesnt exist creates passed entry
    fn delete(&mut self, item: MemtableEntry);

    /// clear all data in the storage
    fn clear(&mut self);

    // TODO: ideally this would return an Iterator
    // possible solution is for structs implementing this trait to have a common iteartor struct
    /// returns all entries
    fn entries(&self) -> Vec<Rc<RefCell<MemtableEntry>>>;
}
