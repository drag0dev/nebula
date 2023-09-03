use std::cmp::Ordering;

#[derive(Ord, Debug, Clone)]
pub struct MemtableEntry {
    /// nanos
    pub timestamp: u128,

    pub key: String,

    /// its value is None it means its a tombstone
    pub value: Option<Vec<u8>>
}

impl MemtableEntry {
    pub fn new_string(timestamp: u128, key: String, value: Option<String>) -> Self {
        let value = if let Some(value) = value {
            Some(value.into_bytes())
        } else {
            None
        };
        MemtableEntry {
            timestamp,
            key,
            value
        }
    }

    pub fn new(timestamp: u128, key: String, value: Option<Vec<u8>>) -> Self {
        MemtableEntry {
            timestamp,
            key,
            value
        }
    }

    /// delete == tombstone
    /// there is no need for timestamp to be updated since all actions happen in memtable inplace
    /// and are older than the entry in the lsm tree if there is one
    pub fn delete(&mut self) {
        self.value = None;
    }

    /// there is no need for timestamp to be updated since all actions happen in memtable inplace
    /// and are older than the entry in the lsm tree if there is one
    pub fn update(&mut self, value: Option<Vec<u8>>) {
        self.value = value;
    }
}

impl PartialEq for MemtableEntry {
    fn eq(&self, other: &MemtableEntry) -> bool {
        self.key == other.key
    }
}
impl Eq for MemtableEntry {}

impl Default for MemtableEntry {
    fn default() -> Self {
        MemtableEntry {
            timestamp: 0,
            key: "".to_string(),
            value: None
        }
    }
}

// TODO: merge sort has to be refactored to work with this
impl PartialOrd for MemtableEntry {
    fn partial_cmp(&self, other: &MemtableEntry) -> Option<Ordering> {
        if self.lt(other) { Some(Ordering::Less) }
        else if self.gt(other) { Some(Ordering::Greater) }
        else { Some(Ordering::Equal) }
    }

    fn lt(&self, other: &MemtableEntry) -> bool {
        self.key < other.key
    }

    fn le(&self, other: &MemtableEntry) -> bool {
        self.key <= other.key
    }

    fn gt(&self, other: &MemtableEntry) -> bool {
        self.key > other.key
    }

    fn ge(&self, other: &MemtableEntry) -> bool {
        self.key >= other.key
    }
}

#[cfg(test)]
mod tests {
    use super::MemtableEntry;

    #[test]
    fn ord_gt() {
        let left = MemtableEntry::new_string(1, "baa".to_string(), None);
        let right = MemtableEntry::new_string(1, "aaa".to_string(), None);
        assert!(left > right);
        assert!(!(right > left));
    }

    #[test]
    fn ord_lt() {
        let left = MemtableEntry::new_string(1, "aaa".to_string(), None);
        let right = MemtableEntry::new_string(1, "baa".to_string(), None);
        assert!(left < right);
        assert!(!(right < left));
    }

    #[test]
    fn ord_ge() {
        let mut left = MemtableEntry::new_string(1, "aaa".to_string(), None);
        let right = MemtableEntry::new_string(1, "baa".to_string(), None);
        assert!(right >= left);
        assert!(!(left >= right));

        left.key = "baa".to_string();
        assert!(right >= left);
        assert!(left >= right);
    }

    #[test]
    fn ord_le() {
        let mut left = MemtableEntry::new_string(1, "aaa".to_string(), None);
        let right = MemtableEntry::new_string(1, "baa".to_string(), None);
        assert!(left <= right);
        assert!(!(right <= left));

        left.key = "baa".to_string();
        assert!(left <= right);
        assert!(right <= left);
    }

    #[test]
    fn ord_neq() {
        let mut left = MemtableEntry::new_string(1, "aaa".to_string(), None);
        let right = MemtableEntry::new_string(1, "baa".to_string(), None);
        assert!(right != left);

        left.key = "baa".to_string();
        assert!(!(left != right));
    }
}
