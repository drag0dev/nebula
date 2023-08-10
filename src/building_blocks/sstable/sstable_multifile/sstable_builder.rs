use std::{
    fs::{
        File,
        create_dir,
        OpenOptions
    },
    io::Write,
    rc::Rc
};
use anyhow::{Result, Context};
use crate::building_blocks::{
    SummaryBuilder,
    IndexBuilder, MemtableEntry,
    Entry, BloomFilter
};

// TODO: metadata
// TODO: syncing file manually

/// SSTable builder where aiding structures are in a separate files
pub struct SSTableBuilderMultiFile {
    index: IndexBuilder,
    summary: SummaryBuilder,
    //metadata: ?,
    filter: BloomFilter,
    filter_file: File,
    sstable_file: File,
    sstable_offset: u64,
    summary_offset: u64,
    index_offset: u64,
    summary_nth: u64,
    entries_written: u64,

    // the first entry in the current range of the summary
    first_entry_range: Option<Rc<Entry>>,

    // need to keep track in order to be able to write total range and last entry in the current range
    first_entry_written: Option<Rc<Entry>>,
    last_entry_written: Option<Rc<Entry>>,
}

impl SSTableBuilderMultiFile {
    /// item_count - number of items to be written
    /// data_dir - directory where SSTables are stored
    /// generation - generation of the SSTable (name of the directory where this SSTable is going to be written to)
    /// filter_fp_prob - filter false positive probability
    /// summary_nth - from SSTable config - how many entries should summary have
    pub fn new(data_dir: &str, generation: &str, item_count: u64, filter_fp_prob: f64, summary_nth: u64) -> Result<Self> {
        let dir_path = format!("{}/{}", data_dir, generation);
        create_dir(&dir_path)
            .context("creating the generation dirctory")?;

        let sstable_file = create_file(&dir_path, "data")?;
        let index_file = create_file(&dir_path, "index")?;
        let summary_file = create_file(&dir_path, "summary")?;
        let filter_file = create_file(&dir_path, "filter")?;
        // let metadata_file = create_file(&dir_path, "metadata")?;

        let index = IndexBuilder::new(index_file);
        let summary = SummaryBuilder::new(summary_file);
        let filter = BloomFilter::new(item_count, filter_fp_prob);

        Ok(SSTableBuilderMultiFile {
            index,
            summary,
            filter,
            filter_file,
            sstable_file,
            sstable_offset: 0,
            summary_offset: 0,
            index_offset: 0,
            summary_nth,
            entries_written: 0,
            first_entry_range: None,
            first_entry_written: None,
            last_entry_written: None,
        })
    }

    pub fn insert(&mut self, entry: &MemtableEntry) -> Result<()> {
        self.entries_written += 1;
        let entry = Rc::new(Entry::from(entry));
        let entry_ser = entry.serialize()?;

        // only happens once
        if self.first_entry_written.is_none() {
            self.first_entry_written = Some(Rc::clone(&entry));
        }

        self.sstable_file.write_all(&entry_ser)
            .context("writign entry into the sstable file")?;

        self.filter.add(&entry.key)?;

        self.index_offset = self.index.add(&entry.key, self.sstable_offset)
            .context("adding index entry")?;

        self.sstable_offset += entry_ser.len() as u64;

        self.last_entry_written = Some(Rc::clone(&entry));

        if self.first_entry_range.is_none() {
            self.first_entry_range = Some(Rc::clone(&entry));
        }

        if self.entries_written as u64 % self.summary_nth == 0 {
            self.summary.add(
                &self.first_entry_range.as_ref().unwrap().key,
                &self.last_entry_written.as_ref().unwrap().key,
                self.summary_offset)
                .context("adding summary entry")?;
            self.first_entry_range = None;
            self.summary_offset = self.index_offset;
        }

        Ok(())
    }

    /// write the last incomplete entry in the summary and the total range
    /// flush the filter to the file
    pub fn finish(&mut self) -> Result<()> {
        if self.summary_offset != self.index_offset {
            assert!(self.first_entry_range.is_some());
            self.summary.add(
                &self.first_entry_range.as_ref().unwrap().key,
                &self.last_entry_written.as_ref().unwrap().key,
                self.summary_offset)
                .context("adding incomplete last summary entry")?;
        }

        assert!(self.first_entry_written.is_some());
        assert!(self.last_entry_written.is_some());
        self.summary.total_range(&self.first_entry_written.as_ref().unwrap().key, &self.last_entry_written.as_ref().unwrap().key)
            .context("writing total range and last entry in the current range")?;

        self.filter.write_to_file(&mut self.filter_file)
            .context("writing filter to the file")?;
        Ok(())
    }

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
