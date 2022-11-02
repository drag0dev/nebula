use anyhow::{Context, Result};
use murmur3::murmur3_x64_128;
use rand::Rng;
use std::io::prelude::*;
use std::io::Cursor;
const EULER_NUMBER: f64 = 2.71828;

pub struct CountMinSketch {
    /// divisor is 2^pow
    pow: u32,

    /// the hash functions should be "pair-wise independent" (?)
    hash_func_count: u64,

    // m
    column_count: u64,

    /// delta
    desired_accuracy: f64,

    /// certainty of achieving the desired accuracy
    certainty: f64,

    // k = ln(1/eps)
    seeds: Vec<u32>,

    // could this be done with arrays somehow?
    // cols: Vec<u64>,
    // rows: Vec<Vec<u64>>,
    matrix: Vec<Vec<u64>>,
}

#[allow(dead_code)]
impl CountMinSketch {
    pub fn new(desired_accuracy: f64, certainty: f64) -> CountMinSketch {
        // rows = ln(1/eps)
        let hash_func_count = (1_f64 / certainty).log(EULER_NUMBER).ceil() as u64;

        let column_count = (EULER_NUMBER / desired_accuracy).ceil() as u64;
        println!("bruh: {}", column_count);

        let (column_count, pow) = closest_pow(column_count);

        let seeds = match load_seeds() {
            Some(seeds) => {
                if hash_func_count != seeds.len() as u64 {
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

                write_seeds(&seeds);
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
            col_index = modulo(hash_result, self.pow, self.column_count);

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
            col_index = modulo(hash_result, self.pow, self.column_count);

            curr = self.matrix[hash_index][col_index];

            if curr < min {
                min = curr;
            }

            hash_index += 1;
        }
        Ok(min)
    }
}

/// finding modulo using right shift
/// only works if the divisor is a power of 2
fn modulo(hash: u128, pow: u32, divisor: u64) -> usize {
    let mut res = hash;
    while res >= divisor as u128 {
        // res = res % divisor
        res = res >> pow;
    }
    res as usize
}

/// find the closest power of 2 that is >= bit_arr_len
/// expected number of items should be considered carefully especially if memory usage is important
fn closest_pow(n: u64) -> (u64, u32) {
    let mut res: u64 = 2;
    let mut pow: u32 = 1;
    while res < n {
        res = res << 1;
        pow += 1;
    }
    (res, pow)
}

fn load_seeds() -> Option<Vec<u32>> {
    let file_contents = std::fs::read_to_string("cms-seeds.txt");
    if file_contents.is_err() {
        return None;
    } else {
        let file_contents = file_contents.unwrap();
        let file_contents = file_contents.trim();
        let mut seeds: Vec<u32> = Vec::with_capacity(file_contents.matches("\n").count());

        for seed in file_contents.split("\n") {
            let seed = seed.parse::<u32>();
            if seed.is_err() {
                panic!("error: parsing a seed \"{:?}\"", seed.err());
            } else {
                seeds.push(seed.unwrap());
            }
        }
        return Some(seeds);
    }
}

fn write_seeds(seeds: &Vec<u32>) {
    let file = std::fs::File::create("cms-seeds.txt");
    if file.is_err() {
        panic!(
            "error: creating \"cms-seeds.txt\" file: \"{:?}\"",
            file.err()
        );
    }
    let mut file = file.unwrap();
    for s in seeds.iter() {
        match file.write_all(format!("{}\n", s).as_bytes()) {
            Ok(_) => {}
            Err(e) => {
                panic!("error: writing a seed \"{}\"", e);
            }
        };
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
            match cms.add(word) {
                Ok(()) => {}
                Err(e) => panic!("error: failed adding word to CountMinSketch \"{}\"", e),
            }
        }

        match cms.count("foo") {
            Ok(val) => {
                assert_eq!(val >= 4, true);
            }
            Err(e) => panic!("error: cms counting broke \"{}\"", e),
        }
    }

    #[test]
    fn count_bar() {
        let mut cms = CountMinSketch::new(0.1, 0.1);
        let testarr = ["foo", "foo", "foo", "bar", "foo", "foo", "foo", "bar"];

        for word in testarr {
            match cms.add(word) {
                Ok(()) => {}
                Err(e) => panic!("error: failed adding word to CountMinSketch \"{}\"", e),
            }
        }

        match cms.count("foo") {
            Ok(val) => {
                assert_eq!(val >= 2, true);
            }
            Err(e) => panic!("error: cms counting broke \"{}\"", e),
        }
    }

    #[test]
    fn empty_cms() {
        let mut cms = CountMinSketch::new(0.1, 0.1);

        let testarr = [];

        for word in testarr {
            match cms.add(word) {
                Ok(()) => {}
                Err(e) => panic!("error: failed adding word to CountMinSketch \"{}\"", e),
            }
        }

        match cms.count("foo") {
            Ok(val) => {
                assert_eq!(val > 0, false);
            }
            Err(e) => panic!("error: cms counting broke\"{}\"", e),
        }
    }

    #[test]
    fn not_contained() {
        let mut cms = CountMinSketch::new(0.1, 0.1);

        // no bigfoot
        let testarr = [
            "tree", "tree", "tree", "tree", "tree", "tree", "tree", "tree", "tree", "tree", "tree",
            "tree", "tree", "tree", "tree", "tree", "tree", "tree", "tree", "tree", "tree", "tree",
        ];

        for word in testarr {
            match cms.add(word) {
                Ok(()) => {}
                Err(e) => panic!("error: failed adding word to CountMinSketch \"{}\"", e),
            }
        }

        match cms.count("bigfoot") {
            Ok(val) => {
                assert_eq!(val > 0, false);
            }
            Err(e) => panic!("error: cms counting broke\"{}\"", e),
        }
    }

    #[test]
    fn find_egg() {
        let mut cms = CountMinSketch::new(0.1, 0.1);

        // 100 hams, 1 egg
        let testarr = [
            "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham",
            "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham",
            "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham",
            "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham",
            "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham",
            "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham",
            "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham",
            "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham", "ham",
            "ham", "ham", "ham", "ham", "egg",
        ];

        for word in testarr {
            match cms.add(word) {
                Ok(()) => {}
                Err(e) => panic!("error: failed adding word to CountMinSketch \"{}\"", e),
            }
        }

        match cms.count("egg") {
            Ok(val) => {
                assert_eq!(val >= 1, true);
            }
            Err(e) => panic!("error: cms counting broke \"{}\"", e),
        }
    }
}
