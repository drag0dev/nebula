use std::{
    fs::{OpenOptions, File, create_dir, remove_dir_all},
    io::{Seek, Write, Read}, path::Path
};
use anyhow::{Result, Context};
use crate::building_blocks::{BloomFilter, IndexBuilder, SummaryBuilder, SSTableBuilderMultiFile, SSTableBuilderSingleFile, Entry};

pub fn generate_test_data() -> Result<()> {
    generate_dir()?;
    generate_data_filter().context("generating data for filter")?;
    generate_data_index().context("generating data for index")?;
    generate_data_summary().context("generating data for summary")?;
    generate_sstable_singlefile().context("generating singlefile sstable")?;
    generate_sstable_multifile().context("generating multifile sstable")?;
    Ok(())
}

fn generate_dir() -> Result<()> {
    let exists = Path::new("./test-data").is_dir();
    if !exists {
        create_dir("./test-data")
            .context("creating test-data dir")?;
    }
    Ok(())
}

fn remove_dir(path: &str) -> Result<()> {
    let exists = Path::new(path).is_dir();
    if exists {
        remove_dir_all(path)?;
    }
    Ok(())
}

fn generate_data_filter() -> Result<()> {
    let mut valid_file = create_file("valid-filter-read")
        .context("creating valid-filter-read file")?;

    let mut invalid_file = create_file("invalid-filter-read")
        .context("creating invalid-filter-read file")?;

    let mut filter = BloomFilter::new(10, 0.01);
    filter.add(b"asd").context("adding entry to the filter")?;

    filter.write_to_file(&mut valid_file)
        .context("writing valid filter to the file")?;

    filter.write_to_file(&mut invalid_file)
        .context("writing invalid filter to the file")?;

    let current_pos = invalid_file.stream_position()
        .context("getting current position of the invalid filter file")?;

    invalid_file.seek(std::io::SeekFrom::Start(current_pos/2))
        .context("seeking to the middle of the invalid filter file")?;

    invalid_file.write_all(b"fff")
        .context("corrupting the invalid filter file")?;

    Ok(())
}

fn generate_data_index() -> Result<()> {
    let mut valid_file = create_file("valid-index-read")
        .context("creating valid-index-read file")?;

    let mut invalid_file = create_file("invalid-index-read")
        .context("creating invalid-index-read file")?;

    let mut index_builder = IndexBuilder::new(valid_file.try_clone().unwrap());

    for i in 0..10 {
        index_builder.add(&i.to_string().into_bytes(), i)
            .expect("error adding index entry");
    }
    index_builder.finish()
        .expect("error finishing index builder");

    valid_file.rewind().context("rewinding the valid index file")?;

    let mut index_ser = Vec::new();
    valid_file.read_to_end(&mut index_ser)
        .context("reading the valid index file")?;

    corrupt(&mut index_ser);
    invalid_file.write_all(&index_ser)
        .context("writing the invalid index file")?;

    Ok(())
}

fn generate_data_summary() -> Result<()> {
    let mut valid_file = create_file("valid-summary-read")
        .context("creating valid-summary-read file")?;

    let mut invalid_file = create_file("invalid-summary-read")
        .context("creating invalid-summary-read file")?;

    let mut invalid_range_file = create_file("invalid-range-summary-read")
        .context("creating invalid-range-summary-read file")?;

    let mut summary_builder = SummaryBuilder::new(valid_file.try_clone().unwrap());
    for i in (0..100).step_by(10) {
        summary_builder.add(&i.to_string().into_bytes(), &(i+9).to_string().into_bytes(), i*10)
         .context("adding summary entry")?;
    }
    summary_builder.total_range(&"0".to_string().into_bytes(), &"99".to_string().into_bytes())
        .context("adding total range")?;

    valid_file.rewind()
        .context("rewinding the valid summary file")?;

    let mut summary_ser = Vec::new();
    valid_file.read_to_end(&mut summary_ser)
        .context("reading the valid summary file")?;

    let mut summary_ser_copy = summary_ser.clone();

    let halfway = summary_ser.len()/2;
    corrupt(&mut summary_ser[..halfway]);
    invalid_file.write_all(&summary_ser)
        .context("writing the invalid summary file")?;

    summary_ser_copy.push(0xf);
    invalid_range_file.write_all(&summary_ser_copy)
        .context("writing the invalid range summary file")?;

    Ok(())
}

fn generate_sstable_multifile() -> Result<()> {
    remove_dir("./test-data/read-valid-sstable-multifile")
        .context("removing the old valid sstable")?;
    remove_dir("./test-data/read-invalid-sstable-multifile")
        .context("removing the old invalid sstable")?;

    let mut sstable_valid = SSTableBuilderMultiFile::new("test-data", "read-valid-sstable-multifile", 100, 0.1, 10)
        .context("creating the valid sstable")?;
    let mut sstable_invalid = SSTableBuilderMultiFile::new("test-data", "read-invalid-sstable-multifile", 100, 0.1, 10)
        .context("creating the invalid sstable")?;

    for i in 0..100 {
        let entry = Entry {
            key: i.to_string().into_bytes(),
            value: Some(i.to_string().into_bytes()),
            timestamp: i,
        };
        sstable_valid.insert(entry.clone())
            .context("inserting entry into the valid sstable")?;
        sstable_invalid.insert(entry)
            .context("inserting entry into the invalid sstable")?;
    }
    sstable_valid.finish().expect("finishing valid sstable");
    sstable_invalid.finish().expect("finishing invalid sstable");

    let mut data_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open("test-data/read-invalid-sstable-multifile/data")
        .context("opening data file of the invalid sstable")?;

    let mut data_ser = Vec::new();
    data_file.read_to_end(&mut data_ser)
        .context("reading data file of the invalid sstable")?;
    data_file.rewind()
        .context("rewinding the data file of the invalid sstable")?;

    corrupt(&mut data_ser);
    data_file.write_all(&data_ser)
        .context("writing the data file of the invalid sstable")?;

    Ok(())
}

fn generate_sstable_singlefile() -> Result<()> {
    remove_dir("./test-data/read-valid-sstable-singlefile")
        .context("removing the old valid sstable")?;
    remove_dir("./test-data/read-invalid-sstable-singlefile")
        .context("removing the old invalid sstable")?;

    let mut sstable_valid = SSTableBuilderSingleFile::new("test-data", "read-valid-sstable-singlefile", 100, 0.1, 10)
        .context("creating the valid sstable")?;
    let mut sstable_invalid = SSTableBuilderSingleFile::new("test-data", "read-invalid-sstable-singlefile", 100, 0.1, 10)
        .context("creating the invalid sstable")?;

    for i in 0..100 {
        let entry = Entry {
            key: i.to_string().into_bytes(),
            value: Some(i.to_string().into_bytes()),
            timestamp: i,
        };
        sstable_valid.insert(entry.clone())
            .context("inserting entry into the valid sstable")?;
        sstable_invalid.insert(entry)
            .context("inserting entry into the invalid sstable")?;
    }
    sstable_valid.finish_data().expect("finishing valid sstable");
    sstable_invalid.finish_data().expect("finishing invalid sstable");

    let mut data_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open("test-data/read-invalid-sstable-singlefile/data")
        .context("opening data file of the invalid sstable")?;

    let mut data_ser = Vec::new();
    data_file.read_to_end(&mut data_ser)
        .context("reading data file of the invalid sstable")?;
    data_file.rewind()
        .context("rewinding the data file of the invalid sstable")?;

    corrupt(&mut data_ser);
    data_file.write_all(&data_ser)
        .context("writing the data file of the invalid sstable")?;

    Ok(())
}

fn create_file(file_name: &str) -> Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(format!("test-data/{}", file_name))?)
}

fn corrupt(data: &mut[u8]) {
    for i in 0..(data.len()/10) {
        data[i*10] = i as u8;
    }
}
