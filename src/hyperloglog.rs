use std::{
    io::Cursor,
    hash::Hasher
};
use anyhow::{Result, Context};
use murmur3::murmur3_x64_128;
use fnv::*;

pub struct HyperLogLog{
    buckets: Vec<u8>,
    number_of_buckets: u32,
    number_of_bits: u8,
    alpha: f64, // correcting constant
}

// TODO: hardcoding a seed?
const MURMUR_SEED: u32 = 1;

impl HyperLogLog{
    /// will panic if number of buckets is not power of two
    pub fn new(number_of_bits: u8) -> Self{
        if number_of_bits < 4 || number_of_bits > 16{
            panic!("Number of bits has to be in range [4, 16]!");
        }
        let number_of_buckets = 2u32.pow(number_of_bits as u32);

        let mut buckets: Vec<u8> = Vec::with_capacity(number_of_buckets as usize);
        for _ in 0..number_of_buckets {
            buckets.push(0);
        }

        HyperLogLog {
            number_of_buckets,
            number_of_bits, buckets,
            alpha: alpha(number_of_bits as u8),
        }
    }

    pub fn add(&mut self, data: &[u8]) -> Result<()>{
        //let hash = murmur3_x64_128(&mut Cursor::new(data), MURMUR_SEED)
        //    .context("error hashing an item")?;
        let mut hasher = FnvHasher::default();
        hasher.write("".as_bytes());
        let hash = hasher.finish() as usize;

        // TODO: any reason to take other 64 bits?
        // taking first 64 significant bits
        // let hash = (hash >> 64) as usize;
        let bucket = hash >> (64 - self.number_of_bits);
        let value = (hash << self.number_of_bits).trailing_zeros() as u8;
        if value > self.buckets[bucket]{
            self.buckets[bucket] = value;
        }
        Ok(())
    }

    pub fn count(&self) -> f64{
        let mut sum: f64 = 0.0;
        for bucket_value in self.buckets.iter(){
            sum += 2f64.powf((*bucket_value as f64) * -1.0);
        }
        self.alpha * (self.number_of_buckets as f64)* ((self.number_of_buckets as f64) / sum)
    }
}

/// function that calculates alpha based on bits
fn alpha(p: u8) -> f64{
    0.7213 / (1.0 + 1.079 / ( 1 << p) as f64)
}

#[cfg(test)]
mod tests{
    use super::*;

    #[test]
    fn cardinality_8_bits(){
        let mut hll = HyperLogLog::new(8);
        hll.add("one".as_bytes());
        hll.add("two".as_bytes());
        hll.add("three".as_bytes());
        hll.add("four".as_bytes());
        assert_eq!(4, hll.count() as u64);
    }
}
