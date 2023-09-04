use std::time::{Duration, Instant};

pub struct TokenBucket {

    // max bucket capacity
    capacity: usize,

    // current number of tokens
    tokens: usize,

    // timestamp of last refill
    last_reset: Instant,

    // bucket refill time
    reset_interval: Duration,
}

impl TokenBucket {
    pub fn new(capacity: usize, reset_interval: Duration) -> Self {
        Self {
            capacity,
            reset_interval,
            tokens: capacity,
            last_reset: Instant::now(),
        }
    }

    pub fn take(&mut self, tokens: usize) -> bool {
        self.reset();
        if tokens <= self.tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    pub fn reset(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_reset);
        
        if elapsed >= self.reset_interval {
            self.tokens = self.capacity;
            self.last_reset = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_token_bucket_take() {
        let reset_interval = Duration::from_secs(2);
        let mut bucket = TokenBucket::new(5, reset_interval);

        assert!(bucket.take(1));
        assert_eq!(bucket.tokens, 4);

        assert!(!bucket.take(10));
        assert_eq!(bucket.tokens, 4);
    }

    #[test]
    fn test_token_bucket_reset() {
        let reset_interval = Duration::from_secs(2);
        let mut bucket = TokenBucket::new(5, reset_interval);

        sleep(Duration::from_secs(3));

        bucket.reset();
        assert_eq!(bucket.tokens, 5);
    }

    // Usage example
    #[test]
    fn test_token_bucket_behavior() {
        let reset_interval = Duration::from_secs(2);
        let mut bucket = TokenBucket::new(5, reset_interval);

        for _ in 0..100 {
            if bucket.take(1) {
                let timestamp = chrono::Local::now();
                println!("{} => OK", timestamp.format("%H:%M:%S"));
            } else {
                let timestamp = chrono::Local::now();
                println!("{} => FAIL...", timestamp.format("%H:%M:%S"));
            }
            sleep(Duration::from_millis(100));
        }
          
        // Initial capacity
        assert_eq!(bucket.capacity, 5); 
        // All tokens consumed due to rate limits
        assert_eq!(bucket.tokens, 0);   
    }
}
