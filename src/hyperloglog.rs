use std::hash::Hasher;
use fnv::*;

pub struct HyperLogLog{
    buckets: Vec<u8>,
    number_of_bits: u8,
    set: f64,
}

const HLL_MIN_PRECISION: u8 = 4;
const HLL_MAX_PRECISION: u8 = 16;

impl HyperLogLog{
    /// will panic if number of buckets is not in range [4,16]
    pub fn new(number_of_bits: u8) -> Self{
        if number_of_bits < HLL_MIN_PRECISION || number_of_bits > HLL_MAX_PRECISION{
            panic!("Number of bits has to be in range [4, 16]!");
        }
        let number_of_buckets = 2u32.pow(number_of_bits as u32);

        let mut buckets: Vec<u8> = Vec::with_capacity(number_of_buckets as usize);
        for _ in 0..number_of_buckets {
            buckets.push(0);
        }

        HyperLogLog {
            number_of_bits,
            buckets,
            set: (2 << number_of_bits) as f64,
        }
    }

    pub fn add(&mut self, data: &[u8]){
        let mut hasher = FnvHasher::default();
        hasher.write(data);
        let hash = hasher.finish();

        let bucket = hash >> (64 - self.number_of_bits);
        let value = (hash << self.number_of_bits).trailing_zeros() + 1;
        if value as u8 > self.buckets[bucket as usize]{
            self.buckets[bucket as usize] = value as u8;
        }
        // println!("hash: {}, bucket: {}, value: {}", hash, bucket, value);
    }

    pub fn count(&self) -> f64{
        let mut sum: f64 = 0.0;
        let mut empty_buckets = 0;
        for bucket_value in self.buckets.iter(){
            sum += (2f64.powf(*bucket_value as f64)).powf(-1.0);
            if *bucket_value == 0{
                empty_buckets += 1;
            }
        }
        let alpha: f64 = 0.7213 / (1.0 + 1.079 / self.set);
        let mut estimation = alpha * self.set.powf(2.0) / sum;
        if estimation <= 2.5*self.set { // small range correction
            if empty_buckets > 0 {
                estimation = self.set * (self.set / empty_buckets as f64).log2();
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
    use rand::random;

    #[test]
    fn cardinality_8_bits(){
        let mut hll = HyperLogLog::new(4);
        for _ in 0..500_000{
            hll.add(&(random::<u32>()).clone().to_be_bytes());
        }

        /*
        hll.add("one".as_bytes());
        hll.add("two".as_bytes());
        hll.add("three".as_bytes());
        hll.add("four".as_bytes());
        hll.add("asdasdasdasdasdads".as_bytes());
        hll.add("stastad".as_bytes());
        */
        assert_eq!(4, hll.count() as u64);
    }
}
