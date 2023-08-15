use std::{
    fs::{File, OpenOptions},
    io::Seek
};
use anyhow::{Result, Context};
use super::SSTableIteratorMultiFile;
use crate::building_blocks::{IndexIterator, SummaryIterator, SummaryEntry, BloomFilter};

pub struct SSTableReaderMultiFile {
    pub filter: BloomFilter,
    index_file: File,
    summary_file: File,
    // metadata: ?,
    sstable_file: File,
}

impl SSTableReaderMultiFile {
    pub fn load(sstabel_dir: &str) -> Result<Self> {
        let filter_file = open_file(sstabel_dir, "filter")
            .context("opening filter file")?;

        let filter = BloomFilter::read_from_file(filter_file)
            .context("reading filter")?;

        let index_file = open_file(sstabel_dir, "index")
            .context("opening index file")?;

        let summary_file = open_file(sstabel_dir, "summary")
            .context("opening summary file")?;

        let sstable_file = open_file(sstabel_dir, "data")
            .context("opening data file")?;

        Ok(SSTableReaderMultiFile {
            filter,
            index_file,
            summary_file,
            sstable_file,
        })
    }

    pub fn iter(&self) -> Result<SSTableIteratorMultiFile> {
        let mut fd = self.sstable_file.try_clone()
            .context("cloning data fd for sstable iter")?;
        fd.rewind().context("rewinding data fd for sstable iter")?;
        Ok(SSTableIteratorMultiFile::iter(fd))
    }

    pub fn index_iter(&self) -> Result<IndexIterator> {
        let mut fd = self.index_file.try_clone()
            .context("cloning index fd for index iter")?;
        fd.rewind().context("rewinding index fd for index iter")?;
        Ok(IndexIterator::iter(fd))
    }

    /// returns the iterator and the global range
    pub fn summary_iter(&self) -> Result<(SummaryIterator, SummaryEntry)> {
        let mut fd = self.summary_file.try_clone()
            .context("cloning summary fd for summary iter")?;
        fd.rewind().context("rewinding summary fd for summary iter")?;
        SummaryIterator::iter(fd)
    }
}

pub fn open_file(dir: &str, name: &str) -> Result<File> {
    let file_path = format!("{}/{}", dir, name);
    let file = OpenOptions::new()
        .read(true)
        .open(file_path)?;
    Ok(file)
}
