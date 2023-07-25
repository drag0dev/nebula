use std::{
    cell::RefCell,
    rc::Rc
};
use super::{
    Memtable,
    MemtableEntry,
    StorageCRUD
};

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
            _ = old_item.replace(item);
        } else {
            self.create(item);
        }
    }

    fn delete(&mut self, item: MemtableEntry) {
        let index = self.iter().position(|entry| entry.borrow().key == item.key);
        if let Some(index) = index {
            _ = self.get(index).unwrap().borrow_mut().value = None;
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
    let items: Vec<Rc<RefCell<MemtableEntry>>> = Vec::new();
    let mut memtable = Memtable::new(items, 10);

    assert!(memtable.read("0".to_string()).is_none());

    let entry = MemtableEntry::new(0, "0".to_string(), Some("0".to_string()));
    memtable.create(entry.clone());

    assert_eq!(memtable.read("0".to_string()), Some(Rc::new(RefCell::new(entry.clone()))));

    memtable.create(entry.clone());

    assert_eq!(memtable.read("0".to_string()), Some(Rc::new(RefCell::new(entry))));
}

#[test]
fn update() {
    let items: Vec<Rc<RefCell<MemtableEntry>>> = Vec::new();
    let mut memtable = Memtable::new(items, 10);

    let mut entry = MemtableEntry::new(0, "0".to_string(), Some("0".to_string()));
    memtable.create(entry.clone());

    assert_eq!(memtable.read("0".to_string()), Some(Rc::new(RefCell::new(entry.clone()))));

    entry.value = Some("1".to_string());
    memtable.update(entry.clone());

    assert_eq!(memtable.read("0".to_string()), Some(Rc::new(RefCell::new(entry))));
}

#[test]
fn delete() {
    let items: Vec<Rc<RefCell<MemtableEntry>>> = Vec::new();
    let mut memtable = Memtable::new(items, 10);

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
