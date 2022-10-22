use std::io::Cursor;
use bitvec::prelude::BitVec;
use murmur3::murmur3_x64_128;
use anyhow::{Result, Context};

// TODO: a lot of unnecessary casting

#[derive(Debug)]
pub struct BloomFilter{
    pub pow: u8,
    pub hash_functions: u8,
    pub bit_arr: BitVec,
    pub bit_arr_len: usize,
    pub seeds: Vec<u32>,
}

impl BloomFilter{
    /// function will panic if number of hash functions and number of seeds does not match
    pub fn new(pow: u8, hash_functions: u8, seeds: Vec<u32>) -> Self{
        if hash_functions != seeds.len() as u8{
            panic!("Number of hash functions has to match number of seeds!");
        }
        let bit_arr_len = 2_usize.pow(pow as u32);
        let mut bit_arr = BitVec::with_capacity(bit_arr_len);

        // zero out the arr
        // TODO: faster?
        for _ in 0..bit_arr_len{
            bit_arr.push(false);
        }
        BloomFilter {
            pow,
            hash_functions,
            bit_arr,
            bit_arr_len,
            seeds,
        }
    }

    pub fn add(&mut self, item: &str) -> Result<()>{
        for seed in self.seeds.iter(){
            let hash_result = murmur3_x64_128(&mut Cursor::new(item), *seed)
                .context("error hashing an item")?;
            self.bit_arr.set(modulo(hash_result, self.pow as usize, self.bit_arr_len), true);
        }
        Ok(())
    }

    pub fn check(&self, item: &str) -> Result<bool>{
        for seed in self.seeds.iter(){
            let hash_result = murmur3_x64_128(&mut Cursor::new(item), *seed)
                .context("error hashing an item")?;
            let bit = self.bit_arr.get(modulo(hash_result, self.pow as usize, self.bit_arr_len))
                .context("error getting bit")?;
            if !bit{
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// finding modulo using right shift
/// only works if the divisor is a power of 2
fn modulo(hash: u128, pow: usize, divisor: usize) -> usize{
    let mut res = hash;
    while res >= divisor as u128{
        // res = res % divisor
        res = res >> pow;
    }
    res as usize
}

#[cfg(test)]
mod tests{
    use super::*;

    #[test]
    fn present_value(){
        let mut bf = BloomFilter::new(20, 3, vec![111, 222, 333]);

        assert_eq!(bf.add("temp").unwrap(), ());
        assert_eq!(bf.check("temp").unwrap(), true);

        assert_eq!(bf.check("temp1").unwrap(), false);
        assert_eq!(bf.add("temp1").unwrap(), ());
        assert_eq!(bf.check("temp1").unwrap(), true);
    }
}
