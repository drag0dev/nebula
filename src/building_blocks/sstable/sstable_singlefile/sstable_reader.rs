use std::{
    fs::{File, OpenOptions},
    io::{Read, SeekFrom, Seek}
};
use anyhow::{Result, Context};
use crate::building_blocks::{SummaryIterator, SummaryEntry, BloomFilter, Entry};
use super::{SSTableHeader, HEADER_SIZE, SSTableIteratorSingleFile, IndexIteratorSingleFile};

pub struct SSTableReaderSingleFile {
    pub header: SSTableHeader,
    file: File,
}

/// each iter uses a same fd, therefore it is not safe to have multiple iterator iterate at the same time
impl SSTableReaderSingleFile {
    pub fn load(sstabel_dir: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .open(format!("{}/data", sstabel_dir))
            .context("opening sstable file")?;
        SSTableReaderSingleFile::read_sstable(file)
    }

    fn read_sstable(mut file: File) -> Result<Self> {
        let mut header_ser = vec![0; HEADER_SIZE as usize];
        file.read_exact(&mut header_ser)
            .context("reading sstable header")?;

        let header = SSTableHeader::deserialize(&header_ser[..])
            .context("deserializing sstable header")?;

        Ok(Self { header, file })
    }

    pub fn iter(&self) -> Result<SSTableIteratorSingleFile> {
        let mut fd = self.file.try_clone()
            .context("cloning fd")?;
        fd.seek(SeekFrom::Start(self.header.data_offset))
            .context("seeking to data")?;
        Ok(SSTableIteratorSingleFile::iter(fd, self.header.data_offset, self.header.filter_offset))
    }

    pub fn index_iter(&self) -> Result<IndexIteratorSingleFile> {
        let mut fd = self.file.try_clone()
            .context("cloning fd")?;
        fd.seek(SeekFrom::Start(self.header.index_offset))
            .context("seeking to index")?;
        Ok(IndexIteratorSingleFile::iter(fd, self.header.index_offset, self.header.summary_offset))
    }

    pub fn summary_iter(&self) -> Result<(SummaryIterator, SummaryEntry)> {
        let fd = self.file.try_clone()
            .context("cloning fd")?;
        let (mut iter, range) = SummaryIterator::iter(fd)?;

        // amount_to_be_read is set to filesize-totalrange, subtracting the summary offset leaves
        // the actual number of bytes to be read
        iter.amount_to_be_read -= self.header.summary_offset as i64;
        iter.file.seek(SeekFrom::Start(self.header.summary_offset))
            .context("seeking to summary")?;
        Ok((iter, range))
    }

    pub fn read_filter(&self) -> Result<BloomFilter> {
        let mut fd = self.file.try_clone()
            .context("cloning fd")?;
        fd.seek(SeekFrom::Start(self.header.filter_offset))
            .context("seeking to filter")?;
        Ok(BloomFilter::read_from_file(fd)?)
    }

    pub fn prefix_scan(&self, prefix: &str) -> Result<Vec<Entry>> {
        let (summary_iter, range) = self.summary_iter().context("reading summary")?;
        if !prefix_intersects(prefix.as_bytes(), &range.first_key[..], &range.last_key[..]) {
            return Ok(vec![]);
        }

        let mut index_offset = None;
        for entry in summary_iter {
            let entry = entry.context("reading summary entry")?;
            if prefix_intersects(prefix.as_bytes(), &entry.first_key[..], &entry.last_key[..]) {
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
                if vector_prefix(prefix.as_bytes(), &entry.key[..]) { res.push(entry); }
                else if end(prefix.as_bytes(), &entry.key[..]) { break; }
            }
            Ok(res)

        } else {
            Ok(vec![])
        }
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
