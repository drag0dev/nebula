use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

#[derive(Debug)]
pub struct SimHash {
    simhash: u64,
    stopwords: HashSet<String>,
}

impl SimHash {
    pub fn new(simhash: u64, stopwords: HashSet<String>) -> Self {
        SimHash {
            simhash,
            stopwords,
        }
    }

    pub fn calculate(&mut self, text: &str) {
        let word_counts = self.calculate_word_weights(text);
        self.update_simhash(&word_counts);
    }

    pub fn calculate_word_weights(&self, text: &str) -> HashMap<String, i32> {
        let words: Vec<&str> = text.split_whitespace().collect();

        let mut word_counts = HashMap::new();
        for word in words {
            let cleaned_word = word.to_lowercase().chars().filter(|&c| c.is_alphanumeric()).collect::<String>();
            if !cleaned_word.is_empty() && !self.stopwords.contains(&cleaned_word) {
                *word_counts.entry(cleaned_word).or_insert(0) += 1;
            }
        }

        word_counts
    }

    pub fn update_simhash(&mut self, word_counts: &HashMap<String, i32>) {
        let mut weighted_bits: Vec<i32> = vec![0; 64];

        // Calculate hash values for words and store their counts
        for (word, count) in word_counts {
            let hash = SimHash::hash_string(word);
            let hash_chars: Vec<char> = hash.chars().collect();
            self.update_weighted_bits(&mut weighted_bits, &hash_chars, *count);
        }
        
        self.calculate_fingerprint(&mut weighted_bits);
    }

    pub fn hash_string(s: &str) -> String {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        s.hash(&mut hasher);
        // Formats the hash as a binary string with leading zeros
        format!("{:064b}", hasher.finish()) 
    }

    pub fn update_weighted_bits(&mut self, weighted_bits: &mut Vec<i32>, hash_chars: &[char], count: i32) {
        for bit in 0..64 {
            if hash_chars[bit] == '1' {
                weighted_bits[bit] += count;
            } else {
                weighted_bits[bit] -= count;
            }
        }
    }

    pub fn calculate_fingerprint(&mut self, weighted_bits: &mut Vec<i32>) {
        // Apply threshold to get b-bit fingerprint
        self.simhash = 0;
        for bit in 0..64 {
            if weighted_bits[bit] > 0 {
                self.simhash |= 1 << bit;
            }
        }
    }

    pub fn calculate_from_text(&mut self, text: &str) -> u64 { 
        self.calculate(text);
        self.fingerprint()
    }

    pub fn fingerprint(&self) -> u64 {
        self.simhash
    }

}

pub fn hamming_distance(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}

// Calculate similarity based on the Hamming distance
pub fn similarity(hash1: u64, hash2: u64) -> f64 {
    let distance: f64 = hamming_distance(hash1, hash2) as f64;
    1.0 - (distance / 64.0)  
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_string() {
        let hash = SimHash::hash_string("test");
        // Since the output can be non-deterministic due to hashing, let's just check the length
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn test_update_weighted_bits() {
        let mut simhash = SimHash::new();
        let mut weighted_bits = vec![0; 64];

        // Hash a test word
        let hash_chars: Vec<char> = SimHash::hash_string("test").chars().collect();

        // Update weighted_bits with the test hash and count
        simhash.update_weighted_bits(&mut weighted_bits, &hash_chars, 2);

        // Since the hash is non-deterministic, the index for the '1' bit might vary
        let expected_index = hash_chars.iter().position(|&c| c == '1').unwrap();

        assert_eq!(weighted_bits[expected_index], 2);
    }

    #[test]
    fn test_calculate_fingerprint() {
        let mut simhash = SimHash::new();
        let mut weighted_bits = vec![0; 64];
        weighted_bits[2] = 3;
        simhash.calculate_fingerprint(&mut weighted_bits);

        assert_eq!(simhash.fingerprint(), 1 << 2);
    }

    #[test]
    fn test_similarity() {
        let hash1: u64 = 0b1101101;
        let hash2: u64 = 0b1101110;
        let similarity = similarity(hash1, hash2);

        // Calculate the expected similarity based on Hamming distance
        let expected_similarity = 1.0 - (hamming_distance(hash1, hash2) as f64) / 64.0;

        // Use a small epsilon to account for potential floating-point precision issues
        let epsilon = 1e-6;
        assert!((similarity - expected_similarity).abs() < epsilon);
    }

    #[test]
    fn test_calculate_word_weights() {
        let simhash = SimHash::new();
        let text = "This is a test sentence with a few words.";
        let word_counts = simhash.calculate_word_weights(text);

        assert_eq!(word_counts.get("test"), Some(&1));
        assert_eq!(word_counts.get("sentence"), Some(&1));
        assert_eq!(word_counts.get("few"), Some(&1));
        assert_eq!(word_counts.get("words"), Some(&1));
        assert_eq!(word_counts.get("with"), None); // Stopword
        assert_eq!(word_counts.get("this"), None); // Stopword
        assert_eq!(word_counts.get("is"), None);   // Stopword
        assert_eq!(word_counts.get("a"), None);    // Stopword
    }
    
    #[test]
    fn test_calculate_from_text() {
        let mut simhash = SimHash::new();
        let text = "This is a test sentence.";

        let fingerprint = simhash.calculate_from_text(text);

        // Since the hash is non-deterministic, check that it's not zero
        assert_ne!(fingerprint, 0);
    }

    #[test]
    fn test_full_workflow() {
        let text1 = "test sentence 1";
        let text2 = "test sentence 2";

        let mut simhash1 = SimHash::new();
        let mut simhash2 = SimHash::new();

        simhash1.calculate(text1);
        simhash2.calculate(text2);

        let fingerprint1 = simhash1.fingerprint();
        let fingerprint2 = simhash2.fingerprint();

        // Calculate similarity based on Hamming distance
        let similarity = similarity(fingerprint1, fingerprint2);
        assert!(similarity >= 0.0 && similarity <= 1.0);
    }
}
