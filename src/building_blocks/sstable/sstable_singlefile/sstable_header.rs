use bincode::Options;
use serde::{Serialize, Deserialize};
use anyhow::{Result, Context};
use crate::building_blocks::BINCODE_OPTIONS;

pub static HEADER_SIZE: u64 = 40;

/// all the offsets are the offsets from the beginning from the file including the header
/// itself
#[derive(Serialize, Deserialize, Debug)]
pub struct SSTableHeader {
    /// data_offset is just the size of the SSTableHeader length
    pub data_offset: u64,
    pub filter_offset: u64,
    pub index_offset: u64,
    pub summary_offset: u64,
    pub meta_offset: u64,
}

impl SSTableHeader {
    pub fn new() -> Self {
        SSTableHeader {
            data_offset: 0,
            filter_offset: 0,
            index_offset: 0,
            summary_offset: 0,
            meta_offset: 0
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let header_ser =
        BINCODE_OPTIONS
            .serialize(&self)
            .context("serializing header")?;
        Ok(header_ser)
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let header = BINCODE_OPTIONS
            .deserialize(data)
            .context("deserializing header")?;
        Ok(header)
    }
}
