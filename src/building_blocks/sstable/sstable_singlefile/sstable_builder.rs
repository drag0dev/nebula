use std::{
    fs::{File, OpenOptions, create_dir},
    io::{Write, Seek, SeekFrom}, rc::Rc
};
use anyhow::{Result, Context};
use crate::building_blocks::{MemtableEntry, Entry, IndexBuilder, SummaryBuilder, BloomFilter};
use super::{SSTableHeader, SSTableIteratorSingleFile, IndexIteratorSingleFile};

/// SSTable builder where aiding structures are in the same file as the data itself
/// singlefile sstable is built in steps
/// 1. pass - write all sstable entries and the filter
/// 2. pass - write index by reading previously written entries
/// 3. pass - write summary by reading previously written index entries
/// file layout:
/// ----------------------
/// sstable header
/// data
/// filter
/// index
/// metadata ?
/// summary
/// ----------------------
/// this way of creating a single file sstable is very slow due to a lot of IO ops
/// but its the only way of being able to handle very large sstables
pub struct SSTableBuilderSingleFile {
    header: SSTableHeader,

    /// used for reading previously written data
    reader_file: File,

    /// used for writing to the file, synced after each pass
    writer_file: File,

    file_name: String,
    filter: BloomFilter,

    /// last key written, used for generating summary
    last_key_global: Option<Vec<u8>>,
    summary_nth: u64,
}

impl SSTableBuilderSingleFile {
    pub fn new(data_dir: &str, generation: &str, item_count: u64, filter_fp_prob: f64, summary_nth: u64) -> Result<Self> {
        create_dir(format!("{}/{}", data_dir, generation))
            .context("creating the generation directory")?;

        let file_name = format!("{}/{}/data", data_dir, generation);
        let mut writer_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&file_name)
            .context("creating the sstable file")?;

        let reader_file = OpenOptions::new()
            .read(true)
            .open(&file_name)
            .context("opening the sstable file")?;

        let filter = BloomFilter::new(item_count, filter_fp_prob);
        let mut header = SSTableHeader::new();

        // reserving header space for later
        let header_ser = header.serialize()
            .context("serializing empty header")?;
        writer_file.write_all(&header_ser)
            .context("writing the empty sstable header")?;

        header.data_offset = header_ser.len() as u64;

        Ok(Self { header, reader_file, writer_file, filter, file_name, summary_nth, last_key_global: None})
    }

    pub fn insert_entry(&mut self, entry: &Entry) -> Result<()> {
        self.insert_data(entry)
    }

    pub fn insert_memtable_entry(&mut self, entry: &MemtableEntry) -> Result<()> {
        let entry = Entry::from(entry);
        self.insert_data(&entry)
    }

    fn insert_data(&mut self, entry: &Entry) -> Result<()> {
        let entry_ser = entry.serialize()?;

        self.writer_file.write_all(&entry_ser)
            .context("writing the sstable entry")?;

        self.filter.add(&entry.key)
            .context("adding entry to the filter")?;

        Ok(())
    }

    pub fn finish_data(&mut self) -> Result<()> {
        let filter_offset = self.writer_file.stream_position()
            .context("getting current file position")?;
        self.header.filter_offset = filter_offset;

        self.filter.write_to_file(&mut self.writer_file)
            .context("writing the filter")?;

        // writer_file is flushed so that reader file can seek to the written data
        // when generating the index
        self.writer_file.sync_all()
            .context("syncing the sstable file")?;

        self.generate_index()
            .context("generating the index")?;

        self.generate_summary(self.summary_nth)
            .context("generating the summary")?;

        self.finish()
            .context("finishing the sstable file")?;

        Ok(())
    }

    fn generate_index(&mut self) -> Result<()> {
        let index_offset = self.writer_file.stream_position()
            .context("getting current file position after writing the filter")?;
        self.header.index_offset = index_offset;

        let index_fd = self.writer_file
            .try_clone()
            .context("cloning the writer file fd for index")?;
        let mut index_builder = IndexBuilder::new(index_fd);

        self.reader_file.seek(SeekFrom::Start(self.header.data_offset))
            .context("seeking reader file to data")?;
        let reader_fd = self.reader_file.try_clone()
            .context("cloning the reader file fd for data")?;

        let mut data_iter = SSTableIteratorSingleFile::iter(reader_fd, self.header.data_offset, self.header.filter_offset);
        let mut index_offset = 0;
        let mut next_entry;

        // first iteration outside of a loop in order for entry to be initialized for
        // self.last_key_global assignement
        let mut entry = data_iter.next().expect("there is always atleast one entry")?;
        index_builder.add(&entry.key, index_offset)
            .context("adding index entry")?;
        index_offset = data_iter.iter.current_offset;
        loop {
            next_entry = data_iter.next();
            if next_entry.is_none() { break; }
            entry = next_entry.unwrap()?;
            index_builder.add(&entry.key, index_offset)
                .context("adding index entry")?;
            index_offset = data_iter.iter.current_offset;
        }

        self.last_key_global = Some(entry.key);
        self.writer_file.sync_all()
            .context("syncing the sstable file")?;
        Ok(())
    }

    fn generate_summary(&mut self, summary_nth: u64) -> Result<()> {
        assert!(self.last_key_global.is_some());
        let summary_offset = self.writer_file.stream_position()
            .context("getting current file position after filter writing filter")?;
        self.header.summary_offset = summary_offset;

        self.reader_file.seek(SeekFrom::Start(self.header.index_offset))
            .context("seeking to the index")?;
        let reader_fd = self.reader_file.try_clone()
            .context("cloning the reader file fd for index")?;
        let mut index_iter = IndexIteratorSingleFile::iter(reader_fd, self.header.index_offset, self.header.summary_offset);

        let summary_fd = self.writer_file
            .try_clone()
            .context("cloning the writer file fd for summary")?;
        let mut summary_builder = SummaryBuilder::new(summary_fd);

        let first_entry = Rc::new(index_iter.next().expect("there is always atleast first entry")?);
        let first_key_global = first_entry.key.clone();

        // keeps the first entry in the current range
        let mut first_key_range = Some(Rc::clone(&first_entry));
        // keeps the last written entry
        let mut last_key_range = Rc::new(index_iter.next().expect("there is always atleast second entry")?);
        let mut counter = 2;
        let mut current_range_offset = self.header.index_offset;

        while let Some(entry) = index_iter.next() {
            last_key_range = Rc::new(entry?);
            if first_key_range.is_none() { first_key_range = Some(Rc::clone(&last_key_range)); }
            counter += 1;
            if counter % summary_nth == 0 {
                summary_builder.add(&first_key_range.unwrap().key, &last_key_range.key, current_range_offset)?;
                current_range_offset = index_iter.iter.current_offset;
                first_key_range = None;
            }
        }

        if counter % summary_nth != 0 {
            summary_builder.add(&first_key_range.as_ref().unwrap().key, &last_key_range.key, current_range_offset)?
        }

        summary_builder.total_range(&first_key_global, &self.last_key_global.as_ref().unwrap())?;

        self.writer_file.sync_all()
            .context("syncing the sstable file")?;
        Ok(())
    }

    /// writes the sstable header
    fn finish(&mut self) -> Result<()> {
        self.writer_file.rewind()
            .context("rewiding sstable file")?;

        let header_ser = self.header.serialize()
            .context("serializing empty header")?;

        self.writer_file.write_all(&header_ser)
            .context("writing the empty sstable header")?;

        self.writer_file.sync_all()
            .context("syncing the sstable file")?;

        self.writer_file.sync_all()
            .context("syncing the sstable file")?;
        Ok(())
    }
}
