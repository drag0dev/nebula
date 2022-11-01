use std::io::prelude::*;

pub const EULER_NUMBER: f64 = 2.71828;

/// finding modulo using a mask only works if the divisor is a power of 2
pub fn modulo(num: u128, divisor: u128) -> usize {
    let mask: u128 = divisor - 1;
    (num & mask) as usize
}

/// find the closest power of 2 that is >= bit_arr_len
/// expected number of items should be considered carefully especially if memory usage is important
pub fn closest_pow(n: u64) -> (u64, u32){
    let mut res: u64 = 2;
    let mut pow: u32 = 1;
    while res < n{
        res = res << 1;
        pow += 1;
    }
    (res, pow)
}


pub fn load_seeds(filename: &str) -> Option<Vec<u32>>{
    let file_contents = std::fs::read_to_string(filename);
    if file_contents.is_err(){
        return None
    }else{
        let file_contents = file_contents.unwrap();
        let file_contents = file_contents.trim();
        let mut seeds: Vec<u32> = Vec::with_capacity(file_contents.matches("\n").count());

        for seed in file_contents.split("\n"){
            let seed = seed.parse::<u32>();
            if seed.is_err(){
                panic!("error: parsing a seed \"{:?}\"", seed.err());
            }else{
                seeds.push(seed.unwrap());
            }
        }
        return Some(seeds);
    }
}

pub fn write_seeds(seeds: &Vec<u32>, filename: &str){
    let file = std::fs::File::create(filename);
    if file.is_err(){
        panic!("error: creating \"seeds.txt\" file: \"{:?}\"", file.err());
    }
    let mut file = file.unwrap();
    for s in seeds.iter(){
        match file.write_all(format!("{}\n", s).as_bytes()){
            Ok(_) => {},
            Err(e) => {
                panic!("error: writing a seed \"{}\"", e);
            }
        };
    }
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
        assert_eq!(closest_pow(7), (8, 3));
        assert_eq!(closest_pow(2), (2, 1));
        assert_eq!(closest_pow(120), (128, 7));
        assert_eq!(closest_pow(240), (256, 8));
    }
    #[test]
    fn writing_and_reading_seeds(){
        write_seeds(&vec![1, 2, 3], "seeds.txt");
        assert_eq!(load_seeds("seeds.txt").unwrap(), vec![1, 2, 3]);
        write_seeds(&vec![111, 222, 333], "seeds.txt");
        assert_eq!(load_seeds("seeds.txt").unwrap(), vec![111, 222, 333]);
    }
}
