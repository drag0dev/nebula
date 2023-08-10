use crate::building_blocks::{BINCODE_OPTIONS, MAX_KEY_LEN};
use super::IndexEntry;
use anyhow::{Result, Context, anyhow};
use bincode::Options;
use std::{fs::File, io::{Read, Seek, SeekFrom}};

pub struct IndexIterator {
    file: File,
    pub (in crate::building_blocks::sstable) current_offset: u64,
}

impl IndexIterator {
    pub fn iter(file: File) -> Self {
        IndexIterator { file, current_offset: 0 }
    }

    pub fn rewind(&mut self) -> Result<()> {
        self.file.rewind()
            .context("rewiding index file")?;
        self.current_offset = 0;
        Ok(())
    }

    /// offset from the beginning of the file
    pub fn move_iter(&mut self, offset: u64) -> Result<()> {
        self.file.seek(SeekFrom::Start(offset))
            .context("seeking index file")?;
        self.current_offset = offset;
        Ok(())
    }
}

impl Iterator for IndexIterator {
    type Item = Result<IndexEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut len_ser = vec![0; 8];
        let res = self.file.read_exact(&mut len_ser);
        if let Err(e) = res.as_ref() {
            return match e.kind() {
                std::io::ErrorKind::UnexpectedEof => None,
                _ => Some(
                    Err(res.context("reading entry len").err().unwrap()))
            };
        }

        let len = BINCODE_OPTIONS
            .deserialize(&len_ser)
            .context("deserializing entry len");

        if let Err(e) = len { return Some(Err(e)); }
        let len: u64 = len.unwrap();

        // 64kb at most for the key and 8 bytes more for offset
        if len > MAX_KEY_LEN+8 {
            let e = anyhow!("corrupted entry len");
            return Some(Err(e));
        }

        let mut entry_ser = vec![0; len as usize];
        let res = self.file.read_exact(&mut entry_ser)
            .context("reading entry");
        if let Err(e) = res { return Some(Err(e)) }

        let entry = BINCODE_OPTIONS
            .deserialize(&entry_ser)
            .context("deserializing entry");
        if let Err(e) = entry { return Some(Err(e)); }

        self.current_offset = 8+len;
        Some(entry)
    }
}
