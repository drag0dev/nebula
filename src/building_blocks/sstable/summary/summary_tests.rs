use std::fs::OpenOptions;
use super::{SummaryBuilder, SummaryIterator};

#[test]
fn writing() {
    let file = OpenOptions::new()
        .truncate(true)
        .write(true)
        .create(true)
        .open("test-data/valid-summary-write")
        .expect("error opening 'valid-summary-write'");

    let mut summary_builder = SummaryBuilder::new(file);

    for i in (0..100).step_by(10) {
        assert!(summary_builder.add(&i.to_string().into_bytes(), &(i+9).to_string().into_bytes(), i*10).is_ok());
    }

    // total range
    assert!(summary_builder.total_range(&"0".to_string().into_bytes(), &"99".to_string().into_bytes(), 0).is_ok());
}

#[test]
fn read_valid() {
    let file = OpenOptions::new()
        .read(true)
        .open("test-data/valid-summary-read")
        .expect("error opening 'valid-summary-read'");

    let (summary_iter, global_range) = SummaryIterator::iter(&file)
        .expect("reading summary");

    assert_eq!(global_range.first_key, "0".to_string().into_bytes());
    assert_eq!(global_range.last_key, "99".to_string().into_bytes());
    assert_eq!(global_range.offset, 0);

    let mut index = 0;
    for entry in summary_iter {
        assert!(entry.is_ok());
        let entry = entry.unwrap();
        assert_eq!(entry.first_key, index.to_string().into_bytes());
        assert_eq!(entry.last_key, (index+9).to_string().into_bytes());
        assert_eq!(entry.offset, index*10);
        index += 10;
    }
}

#[test]
fn read_invalid_entry() {
    let file = OpenOptions::new()
        .read(true)
        .open("test-data/invalid-entry-summary-read")
        .expect("error opening 'invalid-entry-summary-read'");

    let (summary_iter, _) = SummaryIterator::iter(&file).unwrap();
    let mut corrupted = false;
    for entry in summary_iter {
        if entry.is_err() {
            corrupted = true;
            break;
        }
    }
    assert!(corrupted);
}

#[test]
fn read_invalid_total_range() {
    let file = OpenOptions::new()
        .read(true)
        .open("test-data/invalid-range-summary-read")
        .expect("error opening 'invalid-range-summary-read'");

    let iter = SummaryIterator::iter(&file);
    assert!(iter.is_err());
}
