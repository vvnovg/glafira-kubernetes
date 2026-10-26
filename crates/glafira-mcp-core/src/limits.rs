//! Лимиты нагрузки на кластер (спец. §10.7).

use tokio::time::Instant;

/// Одновременных операций с кластером на сессию и на все сессии.
pub const SESSION_CONCURRENCY: usize = 2;
pub const GLOBAL_CONCURRENCY: usize = 4;
pub const CALLS_PER_MINUTE: u32 = 60;

/// Token bucket: `per_minute` вызовов подряд, затем один в `60 / per_minute` секунд.
#[derive(Debug)]
pub struct RateLimiter {
    capacity: f64,
    tokens: f64,
    refill_per_sec: f64,
    last: Instant,
}

impl RateLimiter {
    pub fn new(per_minute: u32) -> Self {
        let capacity = f64::from(per_minute);
        Self {
            capacity,
            tokens: capacity,
            refill_per_sec: capacity / 60.0,
            last: Instant::now(),
        }
    }

    pub fn try_take(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn bucket_refills_over_time() {
        let mut r = RateLimiter::new(60);
        for _ in 0..60 {
            assert!(r.try_take());
        }
        assert!(!r.try_take());
        tokio::time::advance(Duration::from_millis(1100)).await;
        assert!(r.try_take());
        assert!(!r.try_take());
        tokio::time::advance(Duration::from_secs(120)).await;
        for _ in 0..60 {
            assert!(r.try_take());
        }
        assert!(!r.try_take());
    }
}
