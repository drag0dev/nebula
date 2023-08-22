use crate::building_blocks::MemtableEntry;
use core::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::building_blocks::StorageCRUD;

// NOTE: keys are cloned
// node: (key, MemtableEntry(key, value))
pub struct BTree<K, V> {
    inner: BTreeMap<K, V>,
}

impl<K: Ord, V> BTree<K, V> {
    pub fn new() -> Self {
        BTree {
            inner: BTreeMap::new(),
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.inner.insert(key, value)
    }

    pub fn remove(&mut self, key: K) -> Option<V> {
        self.inner.remove(&key)
    }

    pub fn clear(&mut self) {
        self.inner.clear()
    }

    pub fn update(&mut self, key: K, value: V) {
        self.inner.remove(&key);
        self.insert(key, value);
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.inner.values()
    }
}

impl StorageCRUD for BTree<String, Rc<RefCell<MemtableEntry>>> {
    fn create(&mut self, item: MemtableEntry) {
        let res = self.get(&item.key);
        if let Some(old_item) = res {
            _ = old_item.replace(item);
        } else {
            let ckey = item.key.clone();
            self.insert(ckey, Rc::new(RefCell::new(item)));
        }
    }

    fn read(&mut self, key: String) -> Option<Rc<RefCell<MemtableEntry>>> {
        let res = self.get(&key);
        if res.is_some() {
            Some(Rc::clone(res.unwrap()))
        } else {
            None
        }
    }

    fn update(&mut self, item: MemtableEntry) {
        let old_item = self.get(&item.key);
        if let Some(old_item) = old_item {
            _ = old_item.borrow_mut().update(item.value);
        } else {
            self.create(item);
        }
    }

    fn delete(&mut self, item: MemtableEntry) {
        let old_item = self.get(&item.key);
        if let Some(old_item) = old_item {
            _ = old_item.borrow_mut().delete();
        } else {
            self.create(item);
        }
    }

    fn clear(&mut self) {
        self.clear();
    }

    fn entries(&self) -> Vec<Rc<RefCell<MemtableEntry>>> {
        self.values()
            .map(|e| Rc::clone(e))
            .collect::<Vec<Rc<RefCell<MemtableEntry>>>>()
    }
}
