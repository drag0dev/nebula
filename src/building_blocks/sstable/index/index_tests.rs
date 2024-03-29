use std::{
    fs::OpenOptions,
    io::Seek
};
use super::{IndexBuilder, IndexIterator};

#[test]
fn writing() {
    let file = OpenOptions::new()
        .truncate(true)
        .write(true)
        .create(true)
        .open("test-data/valid-index-write")
        .expect("error opening 'valid-index-write'");

    let mut index_builder = IndexBuilder::new(file);

    for i in 0..10 {
        index_builder.add(&i.to_string().into_bytes(), i)
            .expect("error adding index entry");
    }

    // read by offsets
    let mut file = OpenOptions::new()
        .read(true)
        .open("test-data/valid-index-write")
        .expect("error opening 'valid-index-write'");

    // seek to the last offset
    file.seek(std::io::SeekFrom::Start(0)).expect("seeking in the 'valid-index-write'");

    let index_iter = IndexIterator::iter(file);

    for (i, entry) in index_iter.enumerate() {
        let entry = entry.expect("reading index entry");
        assert_eq!(entry.key, i.to_string().into_bytes());
        assert_eq!(entry.offset, i as u64);
    }
}

#[test]
fn read_valid() {
    let file = OpenOptions::new()
        .read(true)
        .open("test-data/valid-index-read")
        .expect("error opening 'valid-index-read'");

    let index_iter = IndexIterator::iter(file);

    let mut index = 0;
    for entry in index_iter {
        assert!(entry.is_ok());
        let entry = entry.unwrap();
        assert_eq!(entry.key, index.to_string().into_bytes());
        assert_eq!(entry.offset, index);
        index += 1;
    }
}

#[test]
fn read_invalid() {
    let file = OpenOptions::new()
        .read(true)
        .open("test-data/invalid-index-read")
        .expect("error opening 'invalid-index-read'");

    let index_iter = IndexIterator::iter(file);

    let mut corrupted = false;
    for entry in index_iter {
        if entry.is_err() {
            corrupted = true;
            break;
        }
    }
    assert!(corrupted);
}
