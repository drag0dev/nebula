use std::fs::File;
use anyhow::{Result, anyhow};
use crate::building_blocks::{IndexIterator, IndexEntry};

/// wrapper around IndexIteartor that tracks the file cursor position
pub struct IndexIteratorSingleFile {
    start_offset: u64,
    end_offset: u64,
    pub(super) iter: IndexIterator,
}

impl IndexIteratorSingleFile {
    /// end offset is the offset at which index ends (summary starts)
    pub fn iter(file: File, start_offset: u64, end_offset: u64) -> Self {
        let mut iter = IndexIterator::iter(file);
        iter.current_offset = start_offset;
        IndexIteratorSingleFile { start_offset, end_offset, iter }
    }

    /// move to the beginning of the index
    pub fn rewind(&mut self) -> Result<()> {
        self.iter.move_iter(self.start_offset)?;
        Ok(())
    }

    /// offset from the beginning of the file (not the index part)
    pub fn move_iter(&mut self, offset: u64) -> Result<()> {
        if offset < self.start_offset || offset >= self.end_offset {
            let e = anyhow!("offset {} is not in range [{}, {}) ", offset, self.start_offset, self.end_offset);
            return Err(e);
        }

        self.iter.move_iter(offset)?;
        Ok(())
    }
}

impl Iterator for IndexIteratorSingleFile {
    type Item = Result<IndexEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iter.current_offset >= self.end_offset {
            return None;
        }
        self.iter.next()
    }
}
