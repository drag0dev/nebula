use crate::building_blocks::BINCODE_OPTIONS;
use bincode::Options;
use std::{fs::File, io::Write};
use serde::{Serialize, Deserialize};
use anyhow::{Result, Context};

#[derive(Serialize, Deserialize)]
pub struct IndexEntry {
    pub key: String,
    pub offset: u64,
}

/// every entry is individually encoded
/// each entry is written in format: serialized length + encoded entry
pub struct IndexBuilder {
    file: File,
}

impl IndexBuilder {
    pub fn new(file: File) -> Self {
        IndexBuilder { file }
    }

    pub fn add(&mut self, key: String, offset: u64) -> Result<()> {
        let entry = IndexEntry { key, offset };
        let entry_ser = BINCODE_OPTIONS
            .serialize(&entry)
            .context("serializing entry")?;

        let entry_len: u64 = entry_ser.len() as u64;
        let mut len_ser = BINCODE_OPTIONS
            .serialize(&entry_len)
            .context("serializing entry len")?;

        self.file.write_all(&len_ser)
            .context("writing entry len to the file")?;

        self.file.write_all(&entry_ser)
            .context("writing entry to the file")?;

        Ok(())
    }
}
