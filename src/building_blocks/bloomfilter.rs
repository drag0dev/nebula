use std::io::Cursor;
use bitvec::prelude::BitVec;
use murmur3::murmur3_x64_128;
use anyhow::{Result, Context};
use rand::Rng;
use crate::utils::helpers::*;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct BloomFilter{
    item_count: u64,
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

        // let seeds = match load_seeds("bf-seeds.txt"){
        //     Some(seeds) => {
        //         if hash_functions != seeds.len() as u64{
        //             panic!("Number of hash functions has to match number of seeds!");
        //         }
        //         seeds
        //     },
        //     None => {
        //         // generate n seeds
        //         let mut seeds: Vec<u32> = Vec::with_capacity(hash_functions as usize);
        //         let mut rng = rand::thread_rng();
        //         for _ in 0..hash_functions{
        //             seeds.push(rng.gen());
        //         }
        //         write_seeds(&seeds, "bf-seeds.txt");
        //         seeds
        //     }
        // };

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

    pub fn add(&mut self, item: &str) -> Result<()>{
        for seed in self.seeds.iter(){
            let hash_result = murmur3_x64_128(&mut Cursor::new(item), *seed)
                .context("error hashing an item")?;
            self.bit_arr.set(modulo(hash_result, self.bit_arr_len as u128), true);
        }
        Ok(())
    }

    pub fn check(&self, item: &str) -> Result<bool>{
        for seed in self.seeds.iter(){
            let hash_result = murmur3_x64_128(&mut Cursor::new(item), *seed)
                .context("error hashing an item")?;
            let bit = self.bit_arr.get(modulo(hash_result, self.bit_arr_len as u128))
                .context("error getting bit")?;
            if !bit{
                return Ok(false);
            } } Ok(true)
    }
}

#[cfg(test)]
mod tests{
    use super::*;

    #[test]
    fn present_value(){
        let mut bf = BloomFilter::new(100_000, 0.02);

        assert_eq!(bf.add("temp").unwrap(), ());
        assert_eq!(bf.check("temp").unwrap(), true);

        assert_eq!(bf.check("temp1").unwrap(), false);
        assert_eq!(bf.add("temp1").unwrap(), ());
        assert_eq!(bf.check("temp1").unwrap(), true);
    }
}
