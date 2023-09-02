use std::collections::{HashMap, VecDeque};

/// LRU cache
pub struct Cache {
    /// None is not used as a placeholder rather it represnts a tombstone
    data: VecDeque<Option<Vec<u8>>>,

    /// max number of entries in the cash
    capacity: u64,

    size: u64,

    /// mapping keys to indices in the data
    mapping: HashMap<Vec<u8>, usize>,
}

impl Cache {
    pub fn new(capacity: u64) -> Self {
        Cache {
            data: VecDeque::with_capacity(capacity as usize),
            capacity,
            size: 0,
            mapping: HashMap::with_capacity(capacity as usize),
        }
    }

    pub fn find(&mut self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let index = self.mapping.get(key);
        if index.is_none() { None }
        else {
            let index = *index.unwrap();

            let element = self.data.remove(index);
            assert!(element.is_some());
            let element = element.unwrap();

            let value = element.clone();
            self.data.push_front(element);

            // update indices of the elements that were moved
            for entry in self.mapping.iter_mut() {
                if *entry.1 < index { *entry.1 += 1; }
            }

            *self.mapping.get_mut(key).unwrap() = 0;
            Some(value)
        }
    }

    /// adds a new entry to the cache or updates an existing entry with the same key
    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) {
        let index = self.mapping.get(key);
        if let Some(index) = index {
            let index = index.clone();
            let mut element = self.data.remove(index).unwrap();
            let new_value = if let Some(value) = value { Some(value.to_owned()) } else { None };
            element = new_value;
            self.data.push_front(element);

            for entry in self.mapping.iter_mut() {
                if *entry.1 < index { *entry.1 += 1; }
            }
            *self.mapping.get_mut(key).unwrap() = 0;
        } else {
            self.check_space();
            for entry in self.mapping.iter_mut() { *entry.1 += 1 }
            self.mapping.insert(key.to_vec(), 0);

            let value = if let Some(value) = value { Some(value.to_owned()) } else { None };
            self.data.push_front(value);
            self.size += 1;
        }
    }

    /// removes an entry that was used the least if there is no space
    fn check_space(&mut self) {
        assert!(self.size <= self.capacity);
        if self.size == self.capacity {
            _ = self.data.pop_back();
            let mut key = None;
            for entry in self.mapping.iter() {
                if entry.1 == &((self.capacity-1) as usize) {
                    key = Some(entry.0.clone());
                    break;
                }
            }
            assert!(key.is_some());
            let key = key.unwrap();
            _ = self.mapping.remove(&key);
            self.size -= 1;
        }
    }
}

mod tests {
    use super::Cache;

    #[test]
    fn adding_and_overflowing() {
        let mut cache = Cache::new(3);

        let key1 = b"key1";
        let key2 = b"key2";
        let key3 = b"key3";
        let key4 = b"key4";

        cache.add(key1, None);
        assert_eq!(cache.size, 1);

        cache.add(key1, None);
        assert_eq!(cache.size, 1);

        cache.add(key2, None);
        assert_eq!(cache.size, 2);

        cache.add(key3, None);
        assert_eq!(cache.size, 3);

        cache.add(key4, None);
        assert_eq!(cache.size, 3);

        assert!(cache.find(key1).is_none());
        assert!(cache.find(key2).is_some());
        assert!(cache.find(key3).is_some());
        assert!(cache.find(key4).is_some());
    }

    #[test]
    fn finding() {
        let mut cache = Cache::new(2);
        assert!(cache.find(b"key").is_none());

        cache.add(b"key", None);
        assert!(cache.find(b"key").is_some());

        cache.add(b"key2", None);
        assert!(cache.find(b"key2").is_some());
    }

    #[test]
    fn updating() {
        let mut cache = Cache::new(2);
        cache.add(b"key", None);
        let value = cache.find(b"key");
        assert!(value.is_some());
        assert_eq!(value, Some(None));

        cache.add(b"key", Some(b"value"));
        let value = cache.find(b"key");
        assert!(value.is_some());
        assert_eq!(value, Some(Some(b"value".to_vec())));
    }
}
