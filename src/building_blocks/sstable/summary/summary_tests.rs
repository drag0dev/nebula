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

    // 'footer'
    assert!(summary_builder.add(&"0".to_string().into_bytes(), &"99".to_string().into_bytes(), 0).is_ok());

    for i in (0..100).step_by(10) {
        assert!(summary_builder.add(&i.to_string().into_bytes(), &(i+9).to_string().into_bytes(), i*10).is_ok());
    }
}

#[test]
fn read_valid() {
    let file = OpenOptions::new()
        .read(true)
        .open("test-data/valid-summary-read")
        .expect("error opening 'valid-summary-read'");

    let mut summary_iter = SummaryIterator::iter(file);

    let footer = summary_iter.next();
    assert!(footer.is_some());
    let footer = footer.unwrap();
    assert!(footer.is_ok());
    let footer = footer.unwrap();
    assert_eq!(footer.first_key, "0".to_string().into_bytes());
    assert_eq!(footer.last_key, "99".to_string().into_bytes());
    assert_eq!(footer.offset, 0);

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
fn read_invalid() {
    let file = OpenOptions::new()
        .read(true)
        .open("test-data/invalid-summary-read")
        .expect("error opening 'invalid-summary-read'");

    let summary_iter = SummaryIterator::iter(file);

    let mut corrupted = false;
    for entry in summary_iter {
        if entry.is_err() {
            corrupted = true;
            break;
        }
    }
    assert!(corrupted);
}
