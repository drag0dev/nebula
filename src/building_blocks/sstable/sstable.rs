use std::{
    fs::{
        File,
        create_dir,
        OpenOptions
    },
    io::Write
};
use anyhow::{Result, Context};
use super::{IndexBuilder, SummaryBuilder, Filter};
use crate::building_blocks::{MemtableEntry, Entry};

// TODO: metadata
// TODO: generation should be &str for compactions?

/// data - Memtable entries from which SSTable and aiding structures it are generated
/// data_dir - directory where SSTables are stored
/// generation - generation of the SSTable (name of the directory where this SSTable is going to be written to)
/// filter_fp_prob - filter false positive probability
/// summary_nth - from SSTable config - how many entries should summary have
pub fn build(data: Vec<MemtableEntry>, data_dir: &str, generation: usize, filter_fp_prob: f64, summary_nth: u64) -> Result<()> {
    assert!(data.len() != 0);

    let dir_path = format!("{}/{}", data_dir, generation);
    create_dir(&dir_path)
        .context("creating the generation dirctory")?;

    let mut sstable_file = create_file(&dir_path, "data")?;
    let index_file = create_file(&dir_path, "index")?;
    let summary_file = create_file(&dir_path, "summary")?;
    let filter_file = create_file(&dir_path, "filter")?;
    // let metadata_file = create_file(&dir_path, "metadata")?;

    let mut index = IndexBuilder::new(index_file);
    let mut summary = SummaryBuilder::new(summary_file);
    let mut filter = Filter::new(data.len() as u64, filter_fp_prob);

    // required for the index
    let mut sstable_offset = 0u64;

    // tracks the index offset for the first_key in the summary entry
    let mut summary_offset = 0u64;

    // adding the range to summary at the beginning
    range_summary(&mut summary, data.first().unwrap(), data.last().unwrap())?;

    let mut first_key = memtable_entry_key_to_vec(&data.first().unwrap());
    let mut index_offset = 0;
    for (i, memtable_entry) in data.iter().enumerate() {
        let entry = Entry::from(memtable_entry);
        let entry_ser = entry.serialize()?;

        sstable_file.write_all(&entry_ser)
            .context("writign entry into the sstable file")?;

        filter.bf.add(&entry.key)?;
        index_offset = index.add(&entry.key, sstable_offset)
            .context("adding index entry")?;

        sstable_offset += entry_ser.len() as u64;

        if i as u64 % summary_nth == 0 {
            summary.add(&first_key, &entry.key, summary_offset)
                .context("adding summary entry")?;
            first_key = entry.key.clone();
            summary_offset = index_offset;
        }
    }

    // if the number of memtable entries is not divisable by summary_nth write the last incomplete entry in the summary
    if summary_offset != index_offset {
        let last_key = memtable_entry_key_to_vec(&data.last().unwrap());
        summary.add(&first_key, &last_key, summary_offset)
            .context("adding incomplete last summary entry")?;
    }

    filter.write_to_file(filter_file)
        .context("writing filter to the file")?;

    Ok(())
}

fn create_file(dir: &str, file_name: &str) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(format!("{}/{}", dir, file_name))
        .context("creating sstable file")?;
    Ok(file)
}

fn range_summary(summary: &mut SummaryBuilder, first: &MemtableEntry, last: &MemtableEntry) -> Result<()> {
    let first_key = memtable_entry_key_to_vec(first);
    let last_key = memtable_entry_key_to_vec(last);
    summary.add(&first_key, &last_key, 0)
        .context("adding range to summary")?;
    Ok(())
}

fn memtable_entry_key_to_vec(entry: &MemtableEntry) -> Vec<u8> {
    entry
        .key
        .chars()
        .map(|c| c as u8)
        .collect::<Vec<u8>>()
}
