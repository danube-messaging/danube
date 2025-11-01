use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Simple token-bucket rate limiter for messages/sec
#[derive(Debug)]
pub struct RateLimiter {
    max_per_sec: u32,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    pub fn new(max_per_sec: u32) -> Self {
        let now = Instant::now();
        Self {
            max_per_sec,
            inner: Mutex::new(Inner {
                tokens: max_per_sec as f64,
                last_refill: now,
            }),
        }
    }

    /// Try to acquire n tokens. Returns true if allowed.
    pub async fn try_acquire(&self, n: u32) -> bool {
        let mut guard = self.inner.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(guard.last_refill);
        if elapsed >= Duration::from_secs(1) {
            // Refill proportionally to elapsed seconds, capped at bucket size
            let secs = elapsed.as_secs_f64();
            guard.tokens = (guard.tokens + (self.max_per_sec as f64) * secs)
                .clamp(0.0, self.max_per_sec as f64);
            guard.last_refill = now;
        }
        if guard.tokens >= n as f64 {
            guard.tokens -= n as f64;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn allows_initial_capacity() {
        let rl = RateLimiter::new(5);
        for _ in 0..5 {
            assert!(rl.try_acquire(1).await);
        }
        assert!(!rl.try_acquire(1).await);
    }

    #[tokio::test]
    async fn refills_after_one_second() {
        let rl = RateLimiter::new(2);
        assert!(rl.try_acquire(1).await);
        assert!(rl.try_acquire(1).await);
        assert!(!rl.try_acquire(1).await);
        sleep(Duration::from_millis(1100)).await;
        assert!(rl.try_acquire(1).await);
    }

    #[tokio::test]
    async fn caps_to_bucket_size() {
        let rl = RateLimiter::new(2);
        // drain
        assert!(rl.try_acquire(1).await);
        assert!(rl.try_acquire(1).await);
        assert!(!rl.try_acquire(1).await);
        // wait multiple seconds; bucket should cap at 2
        sleep(Duration::from_millis(2200)).await;
        assert!(rl.try_acquire(1).await);
        assert!(rl.try_acquire(1).await);
        assert!(!rl.try_acquire(1).await);
    }

    #[tokio::test]
    async fn basic_concurrency_respects_capacity() {
        let rl = std::sync::Arc::new(RateLimiter::new(3));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let rl_cl = rl.clone();
            handles.push(tokio::spawn(async move { rl_cl.try_acquire(1).await }));
        }
        let results = futures::future::join_all(handles).await;
        let success = results.into_iter().filter(|r| r.as_ref().unwrap().to_owned()).count();
        assert_eq!(success, 3);
    }
}
