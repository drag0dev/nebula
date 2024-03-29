use std::{
    fs::File,
    io::Write
};
use bincode::Options;
use crc::{CRC_32_JAMCRC, Crc};
use serde::{Serialize, Deserialize};
use anyhow::{Result, Context};
use crate::building_blocks::BINCODE_OPTIONS;

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub offset: u64,
}

/// every entry is individually encoded
/// each entry is written in format: serialized length(8b) + crc(4b) + encoded entry
pub struct IndexBuilder {
    file: File,

    /// tracks at which offset each index entry is written
    pub (in crate::building_blocks::sstable) index_offset: u64,
}

impl IndexBuilder {
    pub fn new(file: File) -> Self {
        IndexBuilder { file, index_offset: 0 }
    }

    pub fn add(&mut self, key: &Vec<u8>, offset: u64) -> Result<()> {
        let entry = IndexEntry { key: key.clone(), offset };
        let entry_ser = BINCODE_OPTIONS
            .serialize(&entry)
            .context("serializing index entry")?;

        let crc = Crc::<u32>::new(&CRC_32_JAMCRC)
            .checksum(&entry_ser[..]);

        let crc_ser = BINCODE_OPTIONS
            .serialize(&crc)
            .context("serializing crc for index entry")?;

        let entry_len: u64 = entry_ser.len() as u64;
        let len_ser = BINCODE_OPTIONS
            .serialize(&entry_len)
            .context("serializing index entry len")?;

        self.file.write_all(&len_ser)
            .context("writing index entry len to the file")?;

        self.file.write_all(&crc_ser)
            .context("writing index entry crc to the file")?;

        self.file.write_all(&entry_ser)
            .context("writing index entry to the file")?;

        // offset moved by a single entry len
        self.index_offset +=
            len_ser.len() as u64 +
            crc_ser.len() as u64 +
            entry_ser.len() as u64;

        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.file.flush()
            .context("flushing index file")?;
        Ok(())
    }
}
