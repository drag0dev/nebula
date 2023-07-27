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

/// writes entries into the index one by one
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

        self.file.write_all(&entry_ser)
            .context("writing entry to a file")?;

        Ok(())
    }
}
