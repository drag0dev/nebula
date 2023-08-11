use crate::building_blocks::BINCODE_OPTIONS;
use bincode::Options;
use std::{fs::File, io::Write};
use serde::{Serialize, Deserialize};
use anyhow::{Result, Context};

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub offset: u64,
}

/// every entry is individually encoded
/// each entry is written in format: serialized length + encoded entry
/// offset tracks at which offset each index entry is written
pub struct IndexBuilder {
    file: File,
    index_offset: u64,
}

impl IndexBuilder {
    pub fn new(file: File) -> Self {
        IndexBuilder { file, index_offset: 0 }
    }

    pub fn add(&mut self, key: &Vec<u8>, offset: u64) -> Result<u64> {
        let entry = IndexEntry { key: key.clone(), offset };
        let entry_ser = BINCODE_OPTIONS
            .serialize(&entry)
            .context("serializing entry")?;

        let entry_len: u64 = entry_ser.len() as u64;
        let len_ser = BINCODE_OPTIONS
            .serialize(&entry_len)
            .context("serializing entry len")?;

        self.file.write_all(&len_ser)
            .context("writing entry len to the file")?;

        self.file.write_all(&entry_ser)
            .context("writing entry to the file")?;

        let old_offset = self.index_offset;

        // offset moved by a single entry len
        self.index_offset += len_ser.len() as u64 + entry_ser.len() as u64;

        Ok(old_offset)
    }

    pub fn finish(&mut self) -> Result<()> {
        self.file.flush()
            .context("flushing the file")?;
        Ok(())
    }
}
