use std::io::Cursor;
use bitvec::prelude::BitVec;
use murmur3::murmur3_x64_128;
use anyhow::{Result, Context};
use rand::Rng;

// TODO: a lot of unnecessary casting

#[allow(dead_code)]
const EULER_NUMBER: f64 = 2.71828;

#[derive(Debug)]
#[allow(dead_code)]
pub struct BloomFilter{
    item_count: usize,
    /// false positive probability
    fp_prob: f64,
    /// divisor is 2^pow
    pow: usize,
    hash_functions: usize,
    bit_arr: BitVec,
    bit_arr_len: usize,
    seeds: Vec<u32>,
}

impl BloomFilter{
    /// function will panic if number of hash functions and number of seeds does not match
    pub fn new(item_count: usize, fp_prob: f64, seeds: Option<Vec<u32>>) -> (Self, Option<Vec<u32>>){
        // size = -(items * log(probability)) / (log(2)^2)
        let bit_arr_len = -((item_count as f64 * fp_prob.log(EULER_NUMBER)) /
                            (2_f64.log(EULER_NUMBER).powi(2) as f64))
                            .round() as usize;
        let (bit_arr_len, pow) = closest_pow(bit_arr_len);

        // hash functions = (size/item_count) * log(2)
        let hash_functions = ((bit_arr_len as f64 /item_count as f64) * 2_f64.log(EULER_NUMBER))
                            .round() as usize;
        let seeds = match seeds{
            None => {
                // generate n seeds
                let mut seeds: Vec<u32> = Vec::with_capacity(hash_functions);
                let mut rng = rand::thread_rng();
                for _ in 0..hash_functions{
                    seeds.push(rng.gen());
                }
                seeds
            },
            Some(seeds) => {
                if hash_functions != seeds.len() as usize{
                    panic!("Number of hash functions has to match number of seeds!");
                }
                seeds
            }
        };

        let mut bit_arr = BitVec::with_capacity(bit_arr_len as usize);

        // TODO: faster?
        // zero out the arr
        for _ in 0..bit_arr_len{
            bit_arr.push(false);
        }

        (BloomFilter {
            pow,
            hash_functions,
            bit_arr,
            bit_arr_len,
            seeds: seeds.clone(),
            fp_prob,
            item_count,
        }, Some(seeds))
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
            } } Ok(true)
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

/// find the closest power of 2 that is >= bit_arr_len
fn closest_pow(n: usize) -> (usize, usize){
    let mut res = 2;
    let mut pow = 1;
    while res < n{
        res = res << 1;
        pow += 1;
    }
    (res, pow)
}


#[cfg(test)]
mod tests{
    use super::*;

    #[test]
    fn present_value(){
        let (mut bf, _) = BloomFilter::new(100_000, 0.02, None);

        assert_eq!(bf.add("temp").unwrap(), ());
        assert_eq!(bf.check("temp").unwrap(), true);

        assert_eq!(bf.check("temp1").unwrap(), false);
        assert_eq!(bf.add("temp1").unwrap(), ());
        assert_eq!(bf.check("temp1").unwrap(), true);
    }
}
