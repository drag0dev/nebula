use anyhow::{Result, Context};
use std::fs::{File, OpenOptions};
use super::{IndexIterator, Filter, SummaryIterator, SummaryEntry, SSTableIterator};

pub struct SSTableReader {
    pub filter: Filter,
    index_file: File,
    summary_file: File,
    // metadata: ?,
    sstable_file: File,
}

impl SSTableReader {
    pub fn load(sstabel_dir: &str) -> Result<Self> {
        let filter_file = open_file(sstabel_dir, "filter")
            .context("opening filter file")?;

        let filter = Filter::read_from_file(filter_file)
            .context("reading filter")?;

        let index_file = open_file(sstabel_dir, "index")
            .context("opening index file")?;

        let summary_file = open_file(sstabel_dir, "summary")
            .context("opening summary file")?;

        let sstable_file = open_file(sstabel_dir, "data")
            .context("opening data file")?;

        Ok(SSTableReader {
            filter,
            index_file,
            summary_file,
            sstable_file,
        })
    }

    pub fn iter(&self) -> SSTableIterator {
        SSTableIterator::iter(&self.sstable_file)
    }

    pub fn index_iter(&self) -> IndexIterator {
        IndexIterator::iter(&self.index_file)
    }

    /// returns the iterator and the global range
    pub fn summary_iter(&self) -> Result<(SummaryIterator, SummaryEntry)> {
        SummaryIterator::iter(&self.summary_file)
    }
}

pub fn open_file(dir: &str, name: &str) -> Result<File> {
    let file_path = format!("{}/{}", dir, name);
    let file = OpenOptions::new()
        .read(true)
        .open(file_path)?;
    Ok(file)
}
