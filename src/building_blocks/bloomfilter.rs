use std::{io::{Cursor, Read, Write}, fs::File};
use bincode::Options;
use bitvec::prelude::BitVec;
use crc::{Crc, CRC_32_JAMCRC};
use murmur3::murmur3_x64_128;
use anyhow::{Result, Context, anyhow};
use rand::Rng;
use crate::utils::helpers::*;
use serde::{Serialize, Deserialize};

use super::BINCODE_OPTIONS;

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct BloomFilter{
    pub item_count: u64,
    /// false positive probability
    fp_prob: f64,
    hash_functions: u64,
    bit_arr: BitVec,
    bit_arr_len: u64,
    seeds: Vec<u32>,
}

impl BloomFilter{
    /// function will panic if number of hash functions and number of seeds does not match
    pub fn new(item_count: u64, fp_prob: f64) -> Self{
        // size = -(items * log(probability)) / (log(2)^2)
        let bit_arr_len = -((item_count as f64 * fp_prob.log(EULER_NUMBER)) /
                            (2_f64.log(EULER_NUMBER).powi(2) as f64))
                            .round() as u64;
        let bit_arr_len = closest_pow(bit_arr_len);

        // hash functions = (size/item_count) * log(2)
        let hash_functions = ((bit_arr_len as f64 /item_count as f64) * 2_f64.log(EULER_NUMBER))
                            .round() as u64;

        let mut seeds: Vec<u32> = Vec::with_capacity(hash_functions as usize);
        let mut rng = rand::thread_rng();
        for _ in 0..hash_functions{
            seeds.push(rng.gen());
        }

        let mut bit_arr = BitVec::with_capacity(bit_arr_len as usize);

        // zero out the arr
        for _ in 0..bit_arr_len{
            bit_arr.push(false);
        }

        BloomFilter {
            hash_functions,
            bit_arr,
            bit_arr_len,
            seeds,
            fp_prob,
            item_count,
        }
    }

    pub fn add(&mut self, item: &[u8]) -> Result<()>{
        for seed in self.seeds.iter(){
            let hash_result = murmur3_x64_128(&mut Cursor::new(item), *seed)
                .context("error hashing an item")?;
            self.bit_arr.set(modulo(hash_result, self.bit_arr_len as u128), true);
        }
        Ok(())
    }

    pub fn check(&self, item: &[u8]) -> Result<bool>{
        for seed in self.seeds.iter(){
            let hash_result = murmur3_x64_128(&mut Cursor::new(item), *seed)
                .context("error hashing an item")?;
            let bit = self.bit_arr.get(modulo(hash_result, self.bit_arr_len as u128))
                .context("error getting bit")?;
            if !bit{
                return Ok(false);
            } } Ok(true)
    }

    pub fn read_from_file(mut file: File) -> Result<Self> {
        let mut len_ser = vec![0; 8];
        file.read_exact(&mut len_ser)
            .context("reading filter")?;
        let len: u64 = BINCODE_OPTIONS
            .deserialize(&len_ser[..])
            .context("deserializing len")?;

        let mut crc_ser = vec![0; 4];
        file.read_exact(&mut crc_ser)
            .context("reading crc")?;
        let file_crc: u32 = BINCODE_OPTIONS
            .deserialize(&crc_ser[..])
            .context("deserializing crc")?;

        let mut filter_ser = vec![0; len as usize];
        file.read_exact(&mut filter_ser)
            .context("reading filter")?;

        // expected crc
        let crc = Crc::<u32>::new(&CRC_32_JAMCRC);
        let expected_crc = crc.checksum(&filter_ser[..]);

        if file_crc != expected_crc {
            return Err(anyhow!("file corrupted"));
        }

        let filter_deser: BloomFilter = BINCODE_OPTIONS
            .deserialize(&filter_ser[..])
            .context("deserializing filter")?;

        Ok(filter_deser)
    }

    pub fn write_to_file(&mut self, file: &mut File) -> Result<()> {
        let filter_ser = BINCODE_OPTIONS
            .serialize(&self)
            .context("serializing bloomfilter")?;

        let crc = Crc::<u32>::new(&CRC_32_JAMCRC).checksum(&filter_ser[..]);

        let ser_crc = BINCODE_OPTIONS.serialize(&crc)
            .context("serializing crc")?;

        let len_ser = BINCODE_OPTIONS.serialize(&filter_ser.len())
            .context("serializing len")?;

        file.write_all(&len_ser[..])
            .context("writing len to the file")?;

        file.write_all(&ser_crc[..])
            .context("writing crc to the file")?;

        file.write_all(&filter_ser[..])
            .context("writing filter to the file")?;

        Ok(())
    }

}

#[cfg(test)]
mod tests{
    use std::fs::OpenOptions;
    use super::*;

    #[test]
    fn present_value(){
        let mut bf = BloomFilter::new(100_000, 0.02);

        assert_eq!(bf.add(b"temp").unwrap(), ());
        assert_eq!(bf.check(b"temp").unwrap(), true);

        assert_eq!(bf.check(b"temp1").unwrap(), false);
        assert_eq!(bf.add(b"temp1").unwrap(), ());
        assert_eq!(bf.check(b"temp1").unwrap(), true);
    }

    #[test]
    fn write() {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open("test-data/valid-filter-write")
            .expect("trying to open 'valid-filter-write'");

        let mut filter = BloomFilter::new(10, 0.01);
        assert!(filter.add(b"asd").is_ok());

        assert!(filter.write_to_file(&mut file).is_ok());
    }

    #[test]
    fn read_valid() {
        let file = OpenOptions::new()
            .read(true)
            .open("test-data/valid-filter-read")
            .expect("trying to open 'valid-filter-read'");

        let filter = BloomFilter::read_from_file(file);
        // assert!(filter.is_ok());
        let filter = filter.unwrap();

        let present = filter.check(b"asd").expect("error checking bf");
        assert!(present);
    }

    #[test]
    fn read_invalid() {
        let file = OpenOptions::new()
            .read(true)
            .open("test-data/invalid-filter-read")
            .expect("trying to open 'invalid-filter-read'");

        let filter = BloomFilter::read_from_file(file);
        assert!(filter.is_err());
    }
}
