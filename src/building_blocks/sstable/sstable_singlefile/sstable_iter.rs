use std::fs::File;
use anyhow::{Result, anyhow};
use crate::building_blocks::{SSTableIteratorMultiFile, Entry};

pub struct SSTableIteratorSingleFile {
    start_offset: u64,
    end_offset: u64,
    pub(super) iter: SSTableIteratorMultiFile,
}

/// wrapper around SSTableIteratorMultiFile that tracks the file cursor position
impl SSTableIteratorSingleFile {
    /// end offset is the offset at which data ends (filter starts)
    pub fn iter(file: File, start_offset: u64, end_offset: u64) -> Self {
        let iter = SSTableIteratorMultiFile::iter(file);
        SSTableIteratorSingleFile { start_offset, end_offset, iter }
    }

    /// move to the beginning of the data
    pub fn rewind(&mut self) -> Result<()> {
        self.iter.move_iter(self.start_offset)?;
        Ok(())
    }

    /// offset from the beginning of the file (not the data part)
    pub fn move_iter(&mut self, offset: u64) -> Result<()> {
        if offset < self.start_offset || offset >= self.end_offset {
            let e = anyhow!("offset {} is not in range [{}, {}) ", offset, self.start_offset, self.end_offset);
            return Err(e);
        }

        self.iter.move_iter(offset)?;
        Ok(())
    }
}

impl Iterator for SSTableIteratorSingleFile {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iter.current_offset >= self.end_offset {
            return None;
        }
        self.iter.next()
    }
}
