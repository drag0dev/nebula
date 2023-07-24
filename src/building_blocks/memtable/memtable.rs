use super::MemtableEntry;

pub struct Memtable <S>
    where S: StorageCRUD<MemtableEntry> + IntoIterator
{
    storage: S,

    /// amount of data in the memtable in bytes
    pub len: u64,

    /// max amount of data to be places inside memtable in bytes
    pub capacity: u64,
}

pub trait StorageCRUD <T> {
    fn create(item: T);
    fn read(key: String) -> Option<T>;

    /// updates an existing item, otherwise creates a new one
    fn update(key: String, value: String);

    /// sets entry tombstone field to true
    fn delete(key: String);
}
