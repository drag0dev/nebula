use std::{
    fs::{File, OpenOptions},
    io::{Read, SeekFrom, Seek}
};
use anyhow::{Result, Context};
use crate::building_blocks::{SummaryIterator, SummaryEntry, BloomFilter};
use super::{SSTableHeader, HEADER_SIZE, SSTableIteratorSingleFile, IndexIteratorSingleFile};

pub struct SSTableReaderSingleFile {
    pub header: SSTableHeader,
    file: File,
}

/// each iter uses a new fd, therefore is safe to have multiple iterator iterate at the same time
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

    pub fn iter(&mut self) -> Result<SSTableIteratorSingleFile> {
        let mut fd = self.file.try_clone()
            .context("cloning fd")?;
        fd.seek(SeekFrom::Start(self.header.data_offset))
            .context("seeking to data")?;
        Ok(SSTableIteratorSingleFile::iter(fd, self.header.data_offset, self.header.filter_offset))
    }

    pub fn index_iter(&mut self) -> Result<IndexIteratorSingleFile> {
        let mut fd = self.file.try_clone()
            .context("cloning fd")?;
        fd.seek(SeekFrom::Start(self.header.index_offset))
            .context("seeking to index")?;
        Ok(IndexIteratorSingleFile::iter(fd, self.header.index_offset, self.header.summary_offset))
    }

    pub fn summary_iter(&mut self) -> Result<(SummaryIterator, SummaryEntry)> {
        todo!()
    }

    pub fn read_filter(&mut self) -> Result<BloomFilter> {
        let mut fd = self.file.try_clone()
            .context("cloning fd")?;
        fd.seek(SeekFrom::Start(self.header.filter_offset))
            .context("seeking to filter")?;
        Ok(BloomFilter::read_from_file(fd)?)
    }
}
