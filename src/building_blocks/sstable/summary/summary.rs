use crate::building_blocks::{MAX_KEY_LEN, BINCODE_OPTIONS};
use bincode::Options;
use std::{fs::File, io::Write};
use serde::{Serialize, Deserialize};
use anyhow::{Result, Context};

/// two keys max len + 8bytes for offset
pub static MAX_SUMMARY_ENTRY_LEN: u64 = 2 * MAX_KEY_LEN + 8;

#[derive(Serialize, Deserialize, Debug)]
pub struct SummaryEntry {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub offset: u64,
}

/// every entry is individually encoded
/// each entry is written in format: serialized length + encoded entry
/// first summary entry is the last and the first key of the index, so 'footer' is
/// actually at the beginning
pub struct SummaryBuilder {
    file: File,
}

impl SummaryBuilder {
    pub fn new(file: File) -> Self {
        SummaryBuilder { file }
    }

    pub fn add(&mut self, first_key: &Vec<u8>, last_key: &Vec<u8>, offset: u64) -> Result<()> {
        let entry = SummaryEntry { first_key: first_key.clone(), last_key: last_key.clone(), offset };
        let entry_ser = BINCODE_OPTIONS
            .serialize(&entry)
            .context("serializing summary entry")?;

        let entry_len: u64 = entry_ser.len() as u64;
        let len_ser = BINCODE_OPTIONS
            .serialize(&entry_len)
            .context("serializing summary entry len")?;

        self.file.write_all(&len_ser[..])
            .context("writing summary entry len")?;

        self.file.write_all(&entry_ser[..])
            .context("writing summary entry")?;

        Ok(())
    }
}
