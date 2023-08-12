use std::{
    fs::File,
    io::Write
};
use bincode::Options;
use crc::{Crc, CRC_32_JAMCRC};
use serde::{Serialize, Deserialize};
use anyhow::{Result, Context};
use crate::building_blocks::{MAX_KEY_LEN, BINCODE_OPTIONS};

/// two keys max len + 8bytes for offset
pub static MAX_SUMMARY_ENTRY_LEN: u64 = 2 * MAX_KEY_LEN + 8;

#[derive(Serialize, Deserialize, Debug)]
pub struct SummaryEntry {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub offset: u64,
}

/// every entry is individually encoded
/// each entry is written in format: serialized length(8b) + crc(4b) + encoded entry
/// total range of the keys is written at the end, in the format: encoded_entry + crc(4b) + serialized len(8b)
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

        let crc = Crc::<u32>::new(&CRC_32_JAMCRC)
            .checksum(&entry_ser[..]);

        let crc_ser = BINCODE_OPTIONS
            .serialize(&crc)
            .context("serializing crc for summary entry")?;

        let entry_len: u64 = entry_ser.len() as u64;
        let len_ser = BINCODE_OPTIONS
            .serialize(&entry_len)
            .context("serializing summary entry len")?;

        self.file.write_all(&len_ser[..])
            .context("writing summary entry len")?;

        self.file.write(&crc_ser[..])
            .context("writing summary entry crc")?;

        self.file.write_all(&entry_ser[..])
            .context("writing summary entry")?;

        Ok(())
    }

    /// this method is expected to be called at the end and no more entries are expected to be written,
    /// otherwise summary won't be readable
    /// length last in order to be able to read from the back of the fail
    pub fn total_range(&mut self, first_key: &Vec<u8>, last_key: &Vec<u8>) -> Result<()> {
        let entry = SummaryEntry { first_key: first_key.clone(), last_key: last_key.clone(), offset: 0};
        let entry_ser = BINCODE_OPTIONS
            .serialize(&entry)
            .context("serializing summary entry")?;

        let crc = Crc::<u32>::new(&CRC_32_JAMCRC)
            .checksum(&entry_ser[..]);

        let crc_ser = BINCODE_OPTIONS
            .serialize(&crc)
            .context("serializing crc for summary entry")?;

        let entry_len: u64 = entry_ser.len() as u64;
        let len_ser = BINCODE_OPTIONS
            .serialize(&entry_len)
            .context("serializing summary entry len")?;

        self.file.write_all(&entry_ser[..])
            .context("writing summary entry")?;

        self.file.write_all(&crc_ser[..])
            .context("writing summary entry crc")?;

        self.file.write_all(&len_ser[..])
            .context("writing summary entry len")?;

        self.file.flush()
            .context("flushing summary to the file")?;
        Ok(())
    }
}
