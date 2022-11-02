use std::io::Cursor;

use crate::utils::*;
use anyhow::{Context, Result};
use murmur3::murmur3_x64_128;
use rand::Rng;

#[allow(dead_code)]
const EULER_NUMBER: f64 = 2.71828;

#[derive(Debug)]
#[allow(dead_code)]
pub struct CountMinSketch {
    /// divisor is 2^pow
    pow: u32,

    /// the hash functions should be "pair-wise independent" (?)
    hash_func_count: u64,
    column_count: u64,

    /// delta
    desired_accuracy: f64,

    /// certainty of achieving the desired accuracy
    certainty: f64,

    // k = ln(1/eps)
    seeds: Vec<u32>,

    matrix: Vec<Vec<u64>>,
}

#[allow(dead_code)]
impl CountMinSketch {
    pub fn new(desired_accuracy: f64, certainty: f64) -> CountMinSketch {
        // rows = ln(1/eps)
        let hash_func_count = (1_f64 / certainty).log(EULER_NUMBER).ceil() as u64;

        let column_count = (EULER_NUMBER / desired_accuracy).ceil() as u64;
        let (column_count, pow) = closest_pow(column_count);
        println!("columns: {}", column_count);
        println!("rows: {}", hash_func_count);

        let seeds = match load_seeds("cms-seeds.txt") {
            Some(seeds) => {
                if hash_func_count != seeds.len() as u64 {
                    println!("seeds: {}", seeds.len());

                    panic!("Number of hash functions has to match number of seeds!");
                }
                seeds
            }

            None => {
                // generate n seeds
                let mut seeds: Vec<u32> = Vec::with_capacity(hash_func_count as usize);
                let mut rng = rand::thread_rng();
                for _ in 0..hash_func_count {
                    seeds.push(rng.gen());
                }

                write_seeds(&seeds, "cms-seeds.txt");
                seeds
            }
        };

        let h = hash_func_count as usize;
        let w = column_count as usize;

        // matrix of 0s
        let matrix = vec![vec![0; w]; h];

        println!("\n\ncolumns: {}", column_count);
        println!("rows: {}", hash_func_count);

        CountMinSketch {
            pow,
            hash_func_count,
            column_count,
            desired_accuracy,
            certainty,
            seeds: seeds.clone(),
            matrix,
        }
    }

    /// compute a hash value for each row in the matrix,
    /// and increment the cell at index hash % column_count,
    /// for every row respectively
    pub fn add(&mut self, item: &str) -> Result<()> {
        // current hash function index
        let mut hash_index = 0;
        let mut col_index;

        for seed in self.seeds.iter() {
            let hash_result =
                murmur3_x64_128(&mut Cursor::new(item), *seed).context("error hashing an item")?;

            // get column index
            col_index = modulo(hash_result, self.column_count as u128);

            self.matrix[hash_index][col_index] += 1;

            hash_index += 1;
        }
        Ok(())
    }

    /// compute a hash value for each row in the matrix
    /// and track the values at index hash % column_count,
    /// for every row respectively, then return the minimum of those values
    pub fn count(&self, item: &str) -> Result<u64> {
        // current hash function index
        let mut hash_index = 0;
        let mut col_index;
        let mut min = u64::MAX;
        let mut curr;

        for seed in self.seeds.iter() {
            let hash_result =
                murmur3_x64_128(&mut Cursor::new(item), *seed).context("error hashing an item")?;

            // get column index
            col_index = modulo(hash_result, self.column_count as u128);

            curr = self.matrix[hash_index][col_index];

            if curr < min {
                min = curr;
            }

            hash_index += 1;
        }
        Ok(min)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_foo() {
        let mut cms = CountMinSketch::new(0.1, 0.1);

        let testarr = ["foo", "foo", "foo", "foo", "bar"];
        for word in testarr {
            cms.add(word).unwrap();
        }

        let val = cms.count("foo").unwrap();
        assert_eq!(val >= 4, true);
    }

    #[test]
    fn count_bar() {
        let mut cms = CountMinSketch::new(0.1, 0.1);
        let testarr = ["foo", "foo", "foo", "bar", "foo", "foo", "foo", "bar"];

        for word in testarr {
            cms.add(word).unwrap();
        }

        let val = cms.count("foo").unwrap();
        assert_eq!(val >= 2, true);
    }

    #[test]
    fn empty_cms() {
        let mut cms = CountMinSketch::new(0.1, 0.1);

        let testarr = [];

        for word in testarr {
            cms.add(word).unwrap();
        }

        let val = cms.count("foo").unwrap();
        assert_eq!(val > 0, false);
    }

    #[test]
    fn not_contained() {
        let mut cms = CountMinSketch::new(0.1, 0.1);

        // no bigfoot
        let testarr = vec!["tree"; 200];

        for word in testarr {
            cms.add(word).unwrap();
        }

        let val = cms.count("bigfoot").unwrap();
        assert_eq!(val > 0, false);
    }

    #[test]
    fn find_egg() {
        let mut cms = CountMinSketch::new(0.1, 0.1);

        // 100 hams and 1 egg
        let mut testarr = vec!["ham"; 100];
        testarr.push("egg");

        for word in testarr {
            cms.add(word).unwrap();
        }

        let val = cms.count("egg").unwrap();
        assert_eq!(val >= 1, true);
    }
}
