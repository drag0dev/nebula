use std::{fs::File, io::{Read, Write}};
use anyhow::{Result, Context, anyhow};
use bincode::Options;
use serde::{Serialize, Deserialize};
use crc::{Crc, CRC_32_JAMCRC};
use crate::building_blocks::{BloomFilter, BINCODE_OPTIONS};

// TODO: BloomFilter and CMS writing its own seed to a file redundant

/// bloomfilter and crc are encoded separately
/// encoded crc is just appended to the encoded bloomfilter
#[derive(Serialize, Deserialize)]
pub struct Filter {
    pub bf: BloomFilter,

    // crc of the bloomfilter it self
    crc: u32,
}

impl Filter {
    pub fn new(item_count: u64, fp_prob: f64) -> Self {
        Filter {
            bf: BloomFilter::new(item_count, fp_prob),
            crc: 0,
        }
    }

    pub fn read_from_file(mut file: File) -> Result<Self> {
        let mut filter_bin = Vec::new();
        let filter_bin_len = file.read_to_end(&mut filter_bin)
            .context("reading filter")?;

        // expected crc
        let crc = Crc::<u32>::new(&CRC_32_JAMCRC);
        let expected_crc = crc.checksum(&filter_bin[..filter_bin_len-4]);

        // crc present in the file
        let file_crc: u32 = BINCODE_OPTIONS.deserialize(&filter_bin[filter_bin_len-4..])
            .context("deserilizing crc")?;

        if file_crc != expected_crc {
            return Err(anyhow!("file corrupted"));
        }

        // NOTE: even though bf and crc are encoded separately it can be decoded all at once
        let filter_deser: Filter = BINCODE_OPTIONS
            .deserialize(&filter_bin[..])
            .context("deserializing filter")?;

        Ok(filter_deser)
    }

    pub fn write_to_file(&mut self, mut file: File) -> Result<()> {
        let mut filter_ser = BINCODE_OPTIONS
            .serialize(&self.bf)
            .context("serializing bloomfilter")?;

        self.crc = Crc::<u32>::new(&CRC_32_JAMCRC).checksum(&filter_ser[..]);

        let ser_crc = BINCODE_OPTIONS.serialize(&self.crc)
            .context("serializing crc")?;

        filter_ser.extend_from_slice(&ser_crc[..]);

        file.write_all(&filter_ser[..])
            .context("writing to the file")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    #[test]
    fn write() {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open("test-data/valid-filter-write")
            .expect("trying to open 'valid-filter-write'");

        let mut filter = Filter::new(10, 0.01);
        assert!(filter.bf.add("asd").is_ok());

        assert!(filter.write_to_file(file).is_ok());
    }

    #[test]
    fn read_valid() {
        let file = OpenOptions::new()
            .read(true)
            .open("test-data/valid-filter-read")
            .expect("trying to open 'valid-filter-read'");

        let filter = Filter::read_from_file(file);
        assert!(filter.is_ok());
        let filter = filter.unwrap();

        let present = filter.bf.check("asd").expect("error checking bf");
        assert!(present);
    }

    #[test]
    fn read_invalid() {
        let file = OpenOptions::new()
            .read(true)
            .open("test-data/invalid-filter-read")
            .expect("trying to open 'invalid-filter-read'");

        let filter = Filter::read_from_file(file);
        assert!(filter.is_err());
    }
}
