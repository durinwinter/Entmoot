//! Live per-identity connection counting, checked against the caps
//! `entmoot_core::quota::QuotaPolicy` resolves from `[[quota]]` config. The
//! policy lookup itself is pure and lives in entmoot-core (hot-reloadable
//! alongside auth/ACL/schema/staleness, see `Broker::reload`); this is just
//! the runtime bookkeeping of how many connections each identity currently
//! holds on this node, the same split `SessionRegistry` and `ChurnGuard`
//! already follow between policy and live state.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
pub struct IdentityConns {
    counts: Mutex<HashMap<String, usize>>,
}

impl IdentityConns {
    /// Try to add one connection for `identity` against `limit` (`None` =
    /// unlimited). Returns false, without counting the attempt, if the
    /// identity is already at its cap.
    pub fn try_admit(&self, identity: &str, limit: Option<usize>) -> bool {
        let mut counts = self.counts.lock().unwrap();
        let count = counts.entry(identity.to_string()).or_insert(0);
        if let Some(limit) = limit {
            if *count >= limit {
                return false;
            }
        }
        *count += 1;
        true
    }

    /// Release one connection for `identity` (called when its connection,
    /// however admitted, ends).
    pub fn release(&self, identity: &str) {
        let mut counts = self.counts.lock().unwrap();
        if let Some(count) = counts.get_mut(identity) {
            *count -= 1;
            if *count == 0 {
                counts.remove(identity);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admits_up_to_the_limit_then_refuses() {
        let c = IdentityConns::default();
        assert!(c.try_admit("plc1", Some(2)));
        assert!(c.try_admit("plc1", Some(2)));
        assert!(!c.try_admit("plc1", Some(2)));
    }

    #[test]
    fn release_frees_a_slot() {
        let c = IdentityConns::default();
        assert!(c.try_admit("plc1", Some(1)));
        assert!(!c.try_admit("plc1", Some(1)));
        c.release("plc1");
        assert!(c.try_admit("plc1", Some(1)));
    }

    #[test]
    fn identities_are_independent() {
        let c = IdentityConns::default();
        assert!(c.try_admit("plc1", Some(1)));
        assert!(c.try_admit("plc2", Some(1)));
    }

    #[test]
    fn none_limit_is_unlimited() {
        let c = IdentityConns::default();
        for _ in 0..1000 {
            assert!(c.try_admit("plc1", None));
        }
    }
}
