//! Connect-time admission control, independent of the hard `max_connections`
//! cap.
//!
//! During a reconnect storm (partition heals, a node restarts) many clients
//! arrive at once; each admitted CONNECT does auth, session attach/rehydrate,
//! and retained-message delivery. A GCRA rate limiter throttles how fast new
//! connections enter that path. When saturated we still finish the MQTT
//! handshake far enough to send `ConnAck(ServiceUnavailable)` before closing —
//! a legible protocol signal a well-behaved client backs off on, instead of a
//! bare TCP refusal that looks like an outage and invites a tighter retry
//! loop.

use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;

pub struct ConnectAdmission {
    limiter: Option<RateLimiter<governor::state::NotKeyed, governor::state::InMemoryState, governor::clock::DefaultClock>>,
}

impl ConnectAdmission {
    /// `rate_per_sec = 0` disables admission control entirely (default,
    /// matching prior behavior). `burst` is clamped to at least `rate_per_sec`
    /// so a single quiet second doesn't leave less headroom than the steady
    /// rate itself.
    pub fn new(rate_per_sec: u32, burst: u32) -> Self {
        let Some(rate) = NonZeroU32::new(rate_per_sec) else {
            return Self { limiter: None };
        };
        let burst = NonZeroU32::new(burst.max(rate_per_sec)).unwrap_or(rate);
        let quota = Quota::per_second(rate).allow_burst(burst);
        Self { limiter: Some(RateLimiter::direct(quota)) }
    }

    /// True if this CONNECT may proceed into auth/session/retained-delivery.
    pub fn admit(&self) -> bool {
        match &self.limiter {
            Some(limiter) => limiter.check().is_ok(),
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_admits_unconditionally() {
        let a = ConnectAdmission::new(0, 0);
        for _ in 0..10_000 {
            assert!(a.admit());
        }
    }

    #[test]
    fn sheds_once_burst_is_exhausted() {
        let a = ConnectAdmission::new(5, 5);
        let admitted = (0..50).filter(|_| a.admit()).count();
        // Burst of 5 admitted immediately, the rest shed within this tight loop.
        assert_eq!(admitted, 5, "expected exactly the burst to be admitted, got {admitted}");
    }
}
