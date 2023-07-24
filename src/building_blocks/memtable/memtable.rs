use super::{
    MemtableEntry,
    StorageCRUD
};

pub struct Memtable <S>
    where S: StorageCRUD<MemtableEntry> + IntoIterator
{
    storage: S,

    /// amount of data in the memtable in bytes
    pub len: u64,

    /// max amount of data to be places inside memtable in bytes
    pub capacity: u64,
}

