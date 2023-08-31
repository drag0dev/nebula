use std::io::prelude::*;

pub const EULER_NUMBER: f64 = 2.71828;

/// finding modulo using a mask only works if the divisor is a power of 2
pub fn modulo(num: u128, divisor: u128) -> usize {
    let mask: u128 = divisor - 1;
    (num & mask) as usize
}

/// find the closest power of 2 that is >= bit_arr_len
#[inline(always)]
pub fn closest_pow(n: u64) -> u64{
    1 << (64 - n.leading_zeros() - n.is_power_of_two() as u32)
}

#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn modulo_test(){
        assert_eq!(modulo(10, 2), 0);
        assert_eq!(modulo(51, 2), 1);
        assert_eq!(modulo(17, 4), 1);
        assert_eq!(modulo(18, 4), 2);
        assert_eq!(modulo(27, 4), 3);
        assert_ne!(modulo(49, 4), 0);
    }
    #[test]
    fn closest_pow_test(){
        assert_eq!(closest_pow(7), 8);
        assert_eq!(closest_pow(2), 2);
        assert_eq!(closest_pow(256), 256);
        assert_eq!(closest_pow(120), 128);
        assert_eq!(closest_pow(240), 256);
        assert_eq!(closest_pow(1000), 1024);
        assert_eq!(closest_pow(1024), 1024);
    }
}
