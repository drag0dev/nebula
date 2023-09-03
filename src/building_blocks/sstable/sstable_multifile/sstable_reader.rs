use std::{
    fs::{File, OpenOptions},
    io::Seek
};
use anyhow::{Result, Context};
use super::SSTableIteratorMultiFile;
use crate::building_blocks::{IndexIterator, SummaryIterator, SummaryEntry, BloomFilter, Entry};

pub struct SSTableReaderMultiFile {
    pub filter: BloomFilter,
    index_file: File,
    summary_file: File,
    metadata_file: File,
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

        let metadata_file = open_file(sstabel_dir, "metadata")
            .context("opening metadata file")?;


        Ok(SSTableReaderMultiFile {
            filter,
            index_file,
            summary_file,
            sstable_file,
            metadata_file
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

    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<Entry>> {
        let (summary_iter, range) = self.summary_iter().context("reading summary")?;
        if !(&range.first_key[..] <= end && &range.last_key[..] >= start) { return Ok(vec![]); }

        let mut index_offset = None;
        for entry in summary_iter {
            let entry = entry.context("reading summary entry")?;
            if &entry.first_key[..] <= end && &range.last_key[..] >= start {
                index_offset = Some(entry.offset);
            }
        }

        if index_offset.is_some() {
            let mut res = Vec::new();
            let mut index_iter = self.index_iter().context("getting index iterator")?;
            index_iter.move_iter(index_offset.unwrap()).context("")?;
            let index_entry = index_iter.next().unwrap().context("reading index entry")?;
            let mut iter = self.iter().context("getting sstable iter")?;
            iter.move_iter(index_entry.offset).context("moving sstable iter")?;

            for entry in iter {
                let entry = entry.context("reading sstable entry")?;
                if &entry.key[..] >= start && &entry.key[..] <= end { res.push(entry); }
                else if &entry.key[..] >= end { break; }
            }
            Ok(res)
        } else {
            Ok(vec![])
        }
    }
}

pub fn open_file(dir: &str, name: &str) -> Result<File> {
    let file_path = format!("{}/{}", dir, name);
    let file = OpenOptions::new()
        .read(true)
        .open(file_path)?;
    Ok(file)
}

fn prefix_intersects(prefix: &[u8], start: &[u8], end: &[u8]) -> bool {
    let prefix_len = prefix.len();
    let start_prefix = &start[..prefix_len];
    let end_prefix = &end[..prefix_len];
    prefix >= start_prefix && prefix <= end_prefix
}

fn vector_prefix(prefix: &[u8], key: &[u8]) -> bool {
    prefix == &key[..prefix.len()]

}

fn end(prefix: &[u8], key: &[u8]) -> bool {
    prefix > &key[..prefix.len()]
}
