use crate::building_blocks::{MemtableEntry, Entry};
use super::{SSTableBuilder, SSTableReader};

#[test]
fn write_sstable_multifile() {
    let mut sstable = SSTableBuilder::new("test-data", "write-sstable-multifile", 100, 0.1, 10)
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

#[test]
fn read_valid_sstable_multifile() {
    let sstable_reader = SSTableReader::load("test-data/read-valid-sstable-multifile")
        .expect("reading sstable");

    let mut i = 0;
    for entry in sstable_reader.iter() {
        let entry = entry.expect("reading entry");
        let expected_entry = Entry {
            key: i.to_string().into_bytes(),
            value: Some(i.to_string().into_bytes()), timestamp: i,
        };

        assert_eq!(entry, expected_entry);

        i += 1;
    }

    let mut data_iter = sstable_reader.iter();

    let random_entry_index = sstable_reader.index_iter()
        .nth(10)
        .unwrap()
        .unwrap();

    let random_entry_read = data_iter
        .move_and_red(random_entry_index.offset)
        .unwrap()
        .unwrap();
    assert_eq!(random_entry_read.key, 10.to_string().into_bytes());
}

#[test]
fn read_invalid_sstable_multifile() {
    let sstable_reader = SSTableReader::load("test-data/read-invalid-sstable-multifile")
        .expect("reading sstable");
    let mut corrupted = false;
    for entry in sstable_reader.iter() {
        if entry.is_err() {
            corrupted = true;
            break;
        }
    }
    assert!(corrupted);
}
