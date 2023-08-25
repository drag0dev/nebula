use std::{
    cell::RefCell,
    rc::Rc, path::Path, fs::remove_dir_all
};
use crate::building_blocks::{BTree, FileOrganization};

use super::{
    Memtable,
    MemtableEntry,
    StorageCRUD
};

// NOTE: only used for testing till skiplist/btree is implemented
impl StorageCRUD for Vec<Rc<RefCell<MemtableEntry>>> {
    fn create(&mut self, item: MemtableEntry) {
        let res = self.iter().find(|entry| entry.borrow().key == item.key);
        if let Some(old_item) = res {
            _ = old_item.replace(item);
        } else {
            let item = RefCell::new(item);
            let item = Rc::new(item);
            self.push(item)
        }
    }

    fn read(&mut self, key: String) -> Option<Rc<RefCell<MemtableEntry>>> {
        let res = self.iter().find(|entry| entry.borrow().key == key);
        if res.is_some() {
            Some(Rc::clone(res.unwrap()))
        } else {
            None
        }
    }

    fn update(&mut self, item: MemtableEntry) {
        let old_item = self.iter().find(|entry| entry.borrow().key == item.key);
        if let Some(old_item) = old_item {
            _ = old_item.borrow_mut().update(item.value);
        } else {
            self.create(item);
        }
    }

    fn delete(&mut self, item: MemtableEntry) {
        let index = self.iter().position(|entry| entry.borrow().key == item.key);
        if let Some(index) = index {
            _ = self.get(index).unwrap().borrow_mut().delete();
        } else {
            self.create(item);
        }
    }

    fn clear(&mut self) {
        self.clear();
    }

    fn entries(&self) -> Vec<Rc<RefCell<MemtableEntry>>> {
        self.iter()
            .map(|item| Rc::clone(item))
            .collect()
    }
}

#[test]
fn create() {
    let items: BTree<String, Rc<RefCell<MemtableEntry>>> = BTree::new();
    let mut memtable = Memtable::new(items, 256, FileOrganization::SingleFile, 0.01, 50, "test-data/".into());

    assert!(memtable.read("0".to_string()).is_none());

    let entry = MemtableEntry::new(0, "0".to_string(), Some("0".to_string()));
    memtable.create(entry.clone());

    assert_eq!(memtable.read("0".to_string()), Some(Rc::new(RefCell::new(entry.clone()))));

    memtable.create(entry.clone());

    assert_eq!(memtable.read("0".to_string()), Some(Rc::new(RefCell::new(entry))));
}

#[test]
fn update() {
    let items: BTree<String, Rc<RefCell<MemtableEntry>>> = BTree::new();
    let mut memtable = Memtable::new(items, 256, FileOrganization::SingleFile, 0.01, 50, "test-data/".into());

    let mut entry = MemtableEntry::new(0, "0".to_string(), Some("0".to_string()));
    memtable.create(entry.clone());

    assert_eq!(memtable.read("0".to_string()), Some(Rc::new(RefCell::new(entry.clone()))));

    entry.value = Some("1".to_string());
    memtable.update(entry.clone());

    assert_eq!(memtable.read("0".to_string()), Some(Rc::new(RefCell::new(entry))));
}

#[test]
fn delete() {
    let items: BTree<String, Rc<RefCell<MemtableEntry>>> = BTree::new();
    let mut memtable = Memtable::new(items, 256, FileOrganization::SingleFile, 0.01, 50, "test-data/".into());

    assert!(memtable.read("0".to_string()).is_none());

    let mut entry = MemtableEntry::new(0, "0".to_string(), Some("0".to_string()));
    memtable.create(entry.clone());

    assert_eq!(memtable.read("0".to_string()), Some(Rc::new(RefCell::new(entry.clone()))));

    // deleting existing one
    entry.value = None;
    memtable.delete(entry.clone());

    assert_eq!(memtable.read("0".to_string()), Some(Rc::new(RefCell::new(entry.clone()))));

    // new tombsone
    entry.key = "1".to_string();
    memtable.delete(entry.clone());

    assert_eq!(memtable.read("1".to_string()), Some(Rc::new(RefCell::new(entry.clone()))));
}

#[test]
fn prefix_scan() {
    let items: BTree<String, Rc<RefCell<MemtableEntry>>> = BTree::new();
    let mut memtable = Memtable::new(items, 256, FileOrganization::SingleFile, 0.01, 50, "test-data/".into());

    let mut entry = MemtableEntry::new(0, "aabc".to_string(), Some("0".to_string()));
    memtable.create(entry.clone());

    entry.key = "aaaa".into();
    memtable.create(entry.clone());

    entry.key = "bcasd".into();
    memtable.create(entry.clone());

    let entries = memtable.prefix_scan("aa".into());
    assert_eq!(entries.len(), 2);

    let entries = memtable.prefix_scan("bc".into());
    assert_eq!(entries.len(), 1);

    let entries = memtable.prefix_scan("da".into());
    assert_eq!(entries.len(), 0);

    // ignores tombstones
    entry.value = None;
    entry.key = "da".into();
    memtable.create(entry.clone());
    let entries = memtable.prefix_scan("da".into());
    assert_eq!(entries.len(), 0);
}

#[test]
fn range_scan() {
    let items: BTree<String, Rc<RefCell<MemtableEntry>>> = BTree::new();
    let mut memtable = Memtable::new(items, 256, FileOrganization::SingleFile, 0.01, 50, "test-data/".into());

    let mut entry = MemtableEntry::new(0, "aabc".to_string(), Some("0".to_string()));
    memtable.create(entry.clone());

    entry.key = "accc".into();
    memtable.create(entry.clone());

    let entries = memtable.range_scan("aaaa".into(), "cccc".into());
    assert_eq!(entries.len(), 2);

    entry.key = "cccd".into();
    memtable.create(entry.clone());

    let entries = memtable.range_scan("aaaa".into(), "cccc".into());
    assert_eq!(entries.len(), 2);

    entry.key = "cccb".into();
    memtable.create(entry.clone());

    let entries = memtable.range_scan("aaaa".into(), "cccc".into());
    assert_eq!(entries.len(), 3);
}

#[test]
fn len() {
    let items: BTree<String, Rc<RefCell<MemtableEntry>>> = BTree::new();
    let mut memtable = Memtable::new(items, 256, FileOrganization::SingleFile, 0.01, 50, "test-data/".into());

    // create
    let mut entry = MemtableEntry::new(0, "aabc".to_string(), Some("0".to_string()));
    memtable.create(entry.clone());
    assert_eq!(memtable.len, 1);

    // delete
    entry.value = None;
    memtable.delete(entry.clone());
    assert_eq!(memtable.len, 2);

    // update
    entry.value = Some("123".into());
    memtable.delete(entry);
    assert_eq!(memtable.len, 3);
}

#[test]
fn flushing() {
    let exists = Path::new("./test-data/memtable").is_dir();
    if exists { remove_dir_all("./test-data/memtable").expect("removing old writen memtable"); }

    let items: BTree<String, Rc<RefCell<MemtableEntry>>> = BTree::new();
    let mut memtable = Memtable::new(items, 2, FileOrganization::SingleFile, 0.01, 50, "test-data/".into());

    let mut entry = MemtableEntry::new(0, "aabc".to_string(), Some("0".to_string()));
    memtable.create(entry.clone());
    entry.key = "aaaa".into();
    let res = memtable.create(entry);

    assert!(res.is_some());
    assert!(res.unwrap().is_ok());
}
