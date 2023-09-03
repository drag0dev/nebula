use anyhow::{Result, Context, anyhow};
use crc::{Crc, CRC_32_JAMCRC};
use serde::{Serialize, Deserialize};
use bincode::Options;
use crate::building_blocks::BINCODE_OPTIONS;
use super::MemtableEntry;

/// |CRC(u32),Timestamp(u128),Tombstone(u8),Key len(u64),Value len(8B),key,value|
/// a single data entry
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct Entry {
    /// nanos
    pub timestamp: u128,

    pub key: Vec<u8>,

    /// value is optional incase of a tombstone
    pub value: Option<Vec<u8>>
}

impl From<&MemtableEntry> for Entry {
    fn from(memtable_entry: &MemtableEntry) -> Self {
        let key = memtable_entry.key
            .chars()
            .map(|c| c as u8)
            .collect::<Vec<u8>>();

        Entry {
            timestamp: memtable_entry.timestamp,
            key,
            value: memtable_entry.value.clone()
        }
    }
}

impl Entry {
    /// entry is serialized in format:
    /// length of the entry(8b)+crc(4b)+entry
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let entry_ser = BINCODE_OPTIONS
            .serialize(&self)
            .context("serializing entry")?;

        let entry_crc = Crc::<u32>::new(&CRC_32_JAMCRC)
            .checksum(&entry_ser[..]);

        let mut len_ser = BINCODE_OPTIONS
            .serialize(&(entry_ser.len() as u64))
            .context("serializing entry len")?;

        let crc_ser = BINCODE_OPTIONS
            .serialize(&entry_crc)
            .context("serializing entry crc")?;

        len_ser.extend_from_slice(&crc_ser[..]);
        len_ser.extend_from_slice(&entry_ser[..]);
        Ok(len_ser)
    }

    /// expected slice: crc(4b)+entry
    pub fn deserialize(entry: &[u8]) -> Result<Entry> {
        let crc_deser: u32 = BINCODE_OPTIONS
            .deserialize(&entry[..4])
            .context("deserializing crc")?;

        let computed_crc = Crc::<u32>::new(&CRC_32_JAMCRC)
            .checksum(&entry[4..]);

        if crc_deser != computed_crc {
            let e = anyhow!("crc does not match")
                .context("deserializing entry");
            return Err(e);
        }

        let entry_deser = BINCODE_OPTIONS
            .deserialize(&entry[4..])
            .context("deserializing entry")?;

        Ok(entry_deser)
    }
}

#[cfg(test)]
mod tests {
    use super::Entry;

    #[test]
    fn ser_deser_with_value() {
        let entry = Entry{timestamp: 123, key: vec![1, 1, 1, 1, 1, 0], value: Some(vec![1, 1, 1])};

        let entry_ser = entry.serialize();
        assert!(entry_ser.is_ok());
        let entry_ser = entry_ser.unwrap();

        let entry_deser = Entry::deserialize(&entry_ser[8..]);
        assert!(entry_deser.is_ok());
        let entry_deser = entry_deser.unwrap();

        assert_eq!(entry, entry_deser);
    }

    #[test]
    fn ser_deser_without_value() {
        let entry = Entry{timestamp: 123, key: vec![1, 1, 1, 1, 1, 0], value: None};

        let entry_ser = entry.serialize();
        assert!(entry_ser.is_ok());
        let entry_ser = entry_ser.unwrap();

        let entry_deser = Entry::deserialize(&entry_ser[8..]);
        assert!(entry_deser.is_ok());
        let entry_deser = entry_deser.unwrap();

        assert_eq!(entry, entry_deser);
    }
}
