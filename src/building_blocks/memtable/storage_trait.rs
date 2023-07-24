/// the underlying structure used for storing memtable_entry implements given trait
pub trait StorageCRUD <T> {
    fn create(&mut self, item: T);

    fn read(&mut self, key: String) -> Option<T>;

    /// updates an existing item, otherwise creates a new one
    fn update(&mut self, item: T);

    /// sets value field to None
    /// if it doesnt exist create add passed entry
    fn delete(&mut self, item: T);

    /// clear all data in the storage
    fn clear(&mut self);

    /// returnes all entries sorted by key
    fn entries_sorted(&self) -> Vec<T>;
}
