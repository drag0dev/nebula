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
        let number_of_buckets = 1 << number_of_bits;

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

        let mask: u64 = (0..self.number_of_bits).into_iter().map(|b| 1 << 63 - b as u64).sum();
        let lower = hash | mask;

        // for 4 bits
        // b  :   1000000000000000000000000000000000000000000000000000000000000000
        // b  :   0100000000000000000000000000000000000000000000000000000000000000
        // b  :   0010000000000000000000000000000000000000000000000000000000000000
        // b  :   0001000000000000000000000000000000000000000000000000000000000000
        // mask:  1111000000000000000000000000000000000000000000000000000000000000
        //
        // hash:  1100001010001001111101000010011100100101010000110101011100100000
        // lower: 1111001010001001111101000010011100100101010000110101011100100000
        //
        // by setting the first n bits to 1,
        // we avoid counting any zeros that are part of the bucket value
        // in case of hashes such as 0b1000_000...000
        // .trailing_zeros() would count the extra 3 zeros in the first 4 bits

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
        // println!("empty buckets: {}", empty_buckets);

        let m: f64 = self.set;
        let alpha: f64 = 0.7213 / (1.0 + 1.079 / m);
        let mut estimation = alpha * m.powf(2.0) / sum;

        // lower bound alternate calculation
        if estimation <= 2.5*m {
            // println!("LOWER");
            if empty_buckets > 0 {
                estimation = m * (m / empty_buckets as f64).log2();
            }
        // upper bound alternate calculation
        }else if estimation > 1.0 / 30.0 * 2.0f64.powf(32.0){
            // println!("UPPER");
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
        // error : bits
        let mut errors: Vec<(f64,u8)> = Vec::new();

        for i in 4..=16 {
            let mut hll = HyperLogLog::new(i);
            let mut temp: String;
            println!("number of buckets {} (bits {})", hll.set, i);

            let samples = 10_000;

            for _ in 0..samples{
                temp = rand::thread_rng()
                    .sample_iter::<char, _>(rand::distributions::Standard)
                    .take(20)
                    .collect();
                hll.add(&temp.as_bytes());
            }

            let estimation = hll.count() as u64;
            let error = ((estimation as f64 - samples as f64).abs() / samples as f64) * 100.0;

            errors.push((error, i));

            println!("bits: {i} expected: {samples}, found: {}, error: {error}%\n" , hll.count() as u64);
        }

        errors.sort_by(|a, b| b.partial_cmp(a).unwrap());

        for (e, b) in &errors {
            println!("error: {:.2}%, bits: {b}", e);
        }

        let last = errors.len() - 1;
        let best = errors[last].0;
        let bits = errors[last].1;

        println!("\nbest result:\nerror {best}% at {bits} bits\n");

        assert!(false);
    }
}
