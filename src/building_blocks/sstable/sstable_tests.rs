use crate::building_blocks::MemtableEntry;
use super::SSTableBuilder;

#[test]
fn write_sstable_multifile() {
    let mut sstable = SSTableBuilder::new("test-data", "write-sstable", 100, 0.1, 10)
        .expect("creating a sstable");

    for i in 0..100 {
        let entry = MemtableEntry {
            key: i.to_string(),
            value: Some(i.to_string()),
            timestamp: i,
        };
        sstable.insert(&entry).expect("inserting entry into the sstable");
    }
    sstable.finish().expect("finishing sstable");
}
