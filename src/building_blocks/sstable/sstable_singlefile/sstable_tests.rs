use std::{path::Path, fs::remove_dir_all};
use crate::building_blocks::Entry;
use super::{SSTableReaderSingleFile, sstable_builder::SSTableBuilderSingleFile};

#[test]
fn write_sstable_singlefile() {
    let exists = Path::new("./test-data/write-sstable-singlefile").is_dir();
    if exists { remove_dir_all("./test-data/write-sstable-singlefile").expect("removing old writen sstable"); }

    let mut sstable = SSTableBuilderSingleFile::new("test-data", "write-sstable-singlefile", 100, 0.1, 10)
        .expect("creating a sstable");

    for i in 0..100 {
        let entry = Entry {
            timestamp: i,
            key: i.to_string().into_bytes(),
            value: Some(i.to_string().into_bytes()),
        };

        sstable.insert_entry(&entry).expect("inserting entry into the sstable");
    }

    sstable.finish_data().expect("finishing sstable");
}

#[test]
fn read_valid_sstable_singlefile() {
    let sstable_reader = SSTableReaderSingleFile::load("test-data/read-valid-sstable-singlefile")
        .expect("reading sstable");

    // test sstable entries
    let mut i = 0;
    for entry in sstable_reader.iter().expect("getting sstable iter") {
        let entry = entry.expect("reading entry");
        let expected_entry = Entry {
            key: i.to_string().into_bytes(),
            value: Some(i.to_string().into_bytes()), timestamp: i,
        };

        assert_eq!(entry, expected_entry);
        i += 1;
    }

    // test index
    let mut data_iter = sstable_reader.iter().expect("getting data iter");
    let random_entry_index = sstable_reader.index_iter().expect("getting index iter")
        .nth(10)
        .unwrap()
        .expect("reading eleventh entry in the index");

    data_iter.move_iter(random_entry_index.offset)
        .expect("moving sstable iter");

    let random_entry_read = data_iter.next().unwrap().expect("reading random sstable entry");
    assert_eq!(random_entry_read.key, 10.to_string().into_bytes());

    // test summary
    let (mut summary_iter, _) = sstable_reader
        .summary_iter()
        .expect("getting summary iter");

    let random_entry_summary = summary_iter
        .nth(5)
        .unwrap()
        .expect("getting fifth entry in the summary");

    let mut index_iter = sstable_reader.index_iter().expect("getting index iter");
    index_iter.move_iter(random_entry_summary.offset).expect("moving index iter");

    let index_entry = index_iter.next().unwrap().expect("reading random index entry");
    assert_eq!(index_entry.key, 50.to_string().into_bytes());

    let filter = sstable_reader.read_filter().expect("getting filter");
    // test filter
    for i in 0..100 {
        let check = filter.check(&i.to_string().into_bytes()).expect("checking key in the filter");
        assert_eq!(check, true);
    }
}

#[test]
fn read_invalid_sstable_singlefile() {
    let sstable_reader = SSTableReaderSingleFile::load("test-data/read-invalid-sstable-singlefile")
        .expect("reading sstable");

    let mut corrupted = false;

    for entry in sstable_reader.iter().expect("getting sstable iter") {
        if entry.is_err() {
            corrupted = true;
            break;
        }
    }
    assert!(corrupted);
}
