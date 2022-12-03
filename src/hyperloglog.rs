use std::io::Cursor;
use murmur3::murmur3_x64_128;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct HyperLogLog{
    buckets: Vec<u8>,
    number_of_bits: u8,
    set: f64,
}

const HLL_MIN_PRECISION: u8 = 4;
const HLL_MAX_PRECISION: u8 = 16;

impl HyperLogLog{
    /// will panic if number of bits is not in range [4,16]
    pub fn new(number_of_bits: u8) -> Self{
        if number_of_bits < HLL_MIN_PRECISION || number_of_bits > HLL_MAX_PRECISION{
            panic!("Number of bits has to be in range [4, 16]!");
        }
        let number_of_buckets = 2 << number_of_bits;

        let mut buckets: Vec<u8> = Vec::with_capacity(number_of_buckets as usize);
        for _ in 0..number_of_buckets {
            buckets.push(0);
        }

        HyperLogLog {
            number_of_bits,
            buckets,
            set: number_of_buckets as f64,
        }
    }

    pub fn add(&mut self, data: &[u8]){
        //let hash = murmur3_x64_128(&mut Cursor::new(data), 420).unwrap();

        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        let hash = hasher.finish();

        // only take 64 bits
        //let hash: u64 = (hash >> 64).try_into().unwrap();

        let bucket = hash >> (64 - self.number_of_bits);
        let lower = hash << self.number_of_bits;
        let zeros = lower.trailing_zeros() as u8 + 1;

        if zeros > self.buckets[bucket as usize]{
            self.buckets[bucket as usize] = zeros;
        }
    }

    pub fn count(&self) -> f64{
        let mut sum: f64 = 0.0;
        let mut empty_buckets = 0;

        for bucket_value in self.buckets.iter(){
            sum += 2f64.powf(-1.0 * *bucket_value as f64);
            if *bucket_value == 0{
                empty_buckets += 1;
            }
        }
        println!("empty buckets: {}", empty_buckets);

        let m: f64 = self.set;
        let alpha: f64 = 0.7213 / (1.0 + 1.079 / m);
        let mut estimation = alpha * m.powf(2.0) / sum;

        if estimation <= 2.5*m { // small range correction
            if empty_buckets > 0 {
                estimation = m * (m / empty_buckets as f64).log2();
            }
        }else if estimation > 1.0 / 30.0 * 2.0f64.powf(32.0){ // large range correction
            estimation = -(2.0f64.powf(32.0)) * (1.0-estimation/2.0f64.powf(32.0)).log2();
        }
        estimation
    }
}

#[cfg(test)]
mod tests{
    use super::*;
    use rand::Rng;

    #[test]
    fn cardinality(){
        for i in 4..=16 {
            let mut hll = HyperLogLog::new(i);
            let mut temp: String;
            println!("\n\nnumber of buckets {} (bits {})", hll.set, i);

            for _ in 0..10_000{
                temp = rand::thread_rng()
                    .sample_iter::<char, _>(rand::distributions::Standard)
                    .take(20)
                    .collect();
                hll.add(&temp.as_bytes());
            }

            println!("expected: 10000, found: {}", hll.count() as u64);
        }
        assert!(false);
    }
}
