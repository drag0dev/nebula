use std::{
    fs::File,
    io::{Seek, SeekFrom, Read}
};
use anyhow::{Result, Context, anyhow};
use bincode::Options;
use crate::building_blocks::{Entry, BINCODE_OPTIONS, MAX_KEY_LEN, MAX_VAL_LEN};

pub struct SSTableIteratorMultiFile {
    sstable_file: File,
    pub (in crate::building_blocks::sstable) current_offset: u64,
}

impl SSTableIteratorMultiFile {
    pub fn iter(file: File) -> Self {
        SSTableIteratorMultiFile { sstable_file: file, current_offset: 0 }
    }

    /// move to the beginning of the file
    pub fn rewind(&mut self) -> Result<()> {
        self.sstable_file.rewind()
            .context("rewinding sstable file")?;
        self.current_offset = 0;
        Ok(())
    }

    /// offset from the beginning of the file
    pub fn move_iter(&mut self, offset: u64) -> Result<()> {
        self.sstable_file.seek(SeekFrom::Start(offset))
            .context("seeking sstable file")?;
        self.current_offset = offset;
        Ok(())
    }
}

impl Iterator for SSTableIteratorMultiFile {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut len_ser = vec![0; 8];
        let res = self.sstable_file.read_exact(&mut len_ser);
        if let Err(e) = res.as_ref() {
            return match e.kind() {
                std::io::ErrorKind::UnexpectedEof => return None,
                _ => Some(Err(res.context("reading length of the entry").err().unwrap()))
            }
        }

        let len = BINCODE_OPTIONS
            .deserialize(&len_ser[..])
            .context("deserializing entry len");
        if let Err(e) = len { return Some(Err(e)); }
        let len: u64 = len.unwrap();

        if len > (MAX_KEY_LEN+4+MAX_VAL_LEN) {
            let e = anyhow!("corrupted entry len");
            return Some(Err(e));
        }

        // +4 for crc
        let mut entry_ser = vec![0; (len+4) as usize];
        let res = self.sstable_file.read_exact(&mut entry_ser)
            .context("reading entry");
        if let Err(e) = res { return Some(Err(e)); }

        let entry = Entry::deserialize(&entry_ser[..])
            .context("deserializing entry");
        if let Err(e) = entry { return Some(Err(e)); }
        let entry = entry.unwrap();

        self.current_offset += 8 + 4 + len;
        Some(Ok(entry))
    }
}
