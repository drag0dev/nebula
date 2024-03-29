use std::{
    fs::File,
    io::{Read, Seek, SeekFrom}
};
use anyhow::{Result, Context, anyhow};
use bincode::Options;
use crc::{Crc, CRC_32_JAMCRC};
use super::{SummaryEntry, MAX_SUMMARY_ENTRY_LEN};
use crate::building_blocks::{BINCODE_OPTIONS, MAX_KEY_LEN};

pub struct SummaryIterator {
    pub (in crate::building_blocks::sstable) file: File,

    /// number of bytes read so far
    amount_read: i64,

    /// number of bytes to be read in total
    pub (in crate::building_blocks::sstable) amount_to_be_read: i64,

}

impl SummaryIterator {
    pub fn iter(file: File) -> Result<(Self, SummaryEntry)> {
        let mut iter = SummaryIterator {
            file,
            amount_read: 0,
            amount_to_be_read: 0,
        };

        let (total_range, file_size) = iter.read_total_range()?;

        iter.amount_to_be_read = file_size;

        Ok((iter, total_range))
    }

    /// read the total range at the end of the file
    /// returns total range and file size without the it
    fn read_total_range(&mut self) -> Result<(SummaryEntry, i64)> {
        let file_size = self.file.seek(SeekFrom::End(-12))
            .context("seeking to the lenght of the total range")?;

        let mut crc_ser = vec![0; 4];
        self.file.read_exact(&mut crc_ser)
            .context("reading crc of the total range")?;

        let crc: u32 = BINCODE_OPTIONS
            .deserialize(&crc_ser)
            .context("deserializing crc of the total range")?;

        let mut len_ser = vec![0; 8];
        self.file.read_exact(&mut len_ser)
            .context("reading length of the total range")?;
        let len = deserialize_len(&len_ser[..])?;

        if len > MAX_KEY_LEN+8 {
            let e = anyhow!("corrupted inedex entry len");
            return Err(e);
        }

        self.file.seek(SeekFrom::End(-(12+len as i64)))
            .context("seeking to the total range")?;
        let mut entry_ser = vec![0; len as usize];
        self.file.read_exact(&mut entry_ser)
            .context("reading total range")?;

        let computed_crc = Crc::<u32>::new(&CRC_32_JAMCRC)
            .checksum(&entry_ser[..]);
        if computed_crc != crc {
            let e = anyhow!("crc does not match for global range")
                .context("deserializing global range");
            return Err(e);
        }

        let entry = deserialize_entry(&entry_ser[..])?;

        self.file.rewind()
            .context("rewiding to the beginning of the file")?;

        Ok((entry, (file_size-len) as i64))
    }
}

impl Iterator for SummaryIterator {
    type Item = Result<SummaryEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        assert!(self.amount_read <= self.amount_to_be_read);
        if self.amount_to_be_read == self.amount_read {
            return None
        }

        let mut len_ser = vec![0; 8];
        let res = self.file.read_exact(&mut len_ser);
        if let Err(e) = res.as_ref() {
            return match e.kind() {
                std::io::ErrorKind::UnexpectedEof => None,
                _ => Some(Err(res.context("reading summary entry len").err().unwrap()))
            };
        }

        let len = deserialize_len(&len_ser[..]);
        if let Err(e) = len { return Some(Err(e)); }
        let len = len.unwrap();

        if len as i64 > (self.amount_to_be_read - self.amount_read) {
            let e = anyhow!("corrupted summary entry len");
            return Some(Err(e));
        }

        let mut crc_ser = vec![0; 4];
        let res = self.file.read_exact(&mut crc_ser);
        if let Err(e) = res { return Some(Err(e.into())); }

        let crc = BINCODE_OPTIONS
            .deserialize(&crc_ser)
            .context("deserializing crc for summary entry");
        if let Err(e) = crc { return Some(Err(e)); }
        let file_crc: u32 = crc.unwrap();

        self.amount_read += 12;

        let mut entry_ser = vec![0; len as usize];
        let res = self.file.read_exact(&mut entry_ser)
            .context("reading summary entry");
        if let Err(e) = res { return Some(Err(e)) }

        let computed_crc = Crc::<u32>::new(&CRC_32_JAMCRC)
            .checksum(&entry_ser[..]);
        if computed_crc != file_crc {
            let e = anyhow!("crc does not match for summary entry")
                .context("deserializing summary entry");
            return Some(Err(e));
        }

        let entry = deserialize_entry(&entry_ser[..]);
        if let Err(e) = entry { return Some(Err(e)); }

        self.amount_read += len as i64;

        Some(entry)
    }
}

fn deserialize_len(len_ser: &[u8]) -> Result<u64> {
    let len = BINCODE_OPTIONS
        .deserialize(len_ser)
        .context("deserializing summary entry len")?;
    if len > MAX_SUMMARY_ENTRY_LEN {
        let e = anyhow!("corrupted summary entry len");
        return Err(e);
    } else {
        Ok(len)
    }
}

fn deserialize_entry(entry_ser: &[u8]) -> Result<SummaryEntry> {
    Ok(BINCODE_OPTIONS
        .deserialize(&entry_ser)
        .context("deserializing summary entry")?)
}
