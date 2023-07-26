use std::cmp::Ordering;

#[derive(PartialEq, Eq, Ord, Debug, Clone)]
pub struct MemtableEntry {
    /// nanos
    pub timestamp: u128,

    pub key: String,

    /// its value is None it means its a tombstone
    pub value: Option<String>
}

impl MemtableEntry {
    pub fn new(timestamp: u128, key: String, value: Option<String>) -> Self {
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
    pub fn update(&mut self, value: Option<String>) {
        self.value = value;
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
        let left = MemtableEntry::new(1, "baa".to_string(), None);
        let right = MemtableEntry::new(1, "aaa".to_string(), None);
        assert!(left > right);
        assert!(!(right > left));
    }

    #[test]
    fn ord_lt() {
        let left = MemtableEntry::new(1, "aaa".to_string(), None);
        let right = MemtableEntry::new(1, "baa".to_string(), None);
        assert!(left < right);
        assert!(!(right < left));
    }

    #[test]
    fn ord_ge() {
        let mut left = MemtableEntry::new(1, "aaa".to_string(), None);
        let right = MemtableEntry::new(1, "baa".to_string(), None);
        assert!(right >= left);
        assert!(!(left >= right));

        left.key = "baa".to_string();
        assert!(right >= left);
        assert!(left >= right);
    }

    #[test]
    fn ord_le() {
        let mut left = MemtableEntry::new(1, "aaa".to_string(), None);
        let right = MemtableEntry::new(1, "baa".to_string(), None);
        assert!(left <= right);
        assert!(!(right <= left));

        left.key = "baa".to_string();
        assert!(left <= right);
        assert!(right <= left);
    }

    #[test]
    fn ord_neq() {
        let mut left = MemtableEntry::new(1, "aaa".to_string(), None);
        let right = MemtableEntry::new(1, "baa".to_string(), None);
        assert!(right != left);

        left.key = "baa".to_string();
        assert!(!(left != right));
    }
}
