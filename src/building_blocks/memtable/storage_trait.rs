/// the underlying structure used for storing memtable_entry implements given trait
pub trait StorageCRUD <T> {
    fn create(item: T);
    fn read(key: String) -> Option<T>;

    /// updates an existing item, otherwise creates a new one
    fn update(key: String, value: String);

    /// sets entry tombstone field to true
    fn delete(key: String);
}
