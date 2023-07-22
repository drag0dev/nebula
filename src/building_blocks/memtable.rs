use super::Entry;

pub struct Memtable <S>
    where S: StorageCRUD<Entry> + IntoIterator
{
    storage: S,

    /// amount of data in the memtable in bytes
    pub len: u64,

    /// max amount of data to be places inside memtable in bytes
    pub capacity: u64,
}

pub trait StorageCRUD <T> {
    fn create(item: T);
    fn read(item: T) -> Option<T>;
    fn update(item: T);
    fn delete(item: T);
}
