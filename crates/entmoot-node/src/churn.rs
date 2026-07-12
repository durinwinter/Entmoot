//! Reconnect-churn quarantine: the behavior-policy counterpart to
//! `admission.rs`'s connect-admission control.
//!
//! Connect-admission control sheds an *aggregate* reconnect storm without
//! caring who any particular client is. This catches the complementary
//! case: one specific client reconnecting ("flapping") more than
//! `max_reconnects` times within a rolling `window`, which admission
//! control's aggregate rate limit may never notice if the rest of the mesh
//! is quiet. A flapping client is quarantined — its CONNECTs refused with
//! `ServiceUnavailable`, the same legible signal admission control uses —
//! for `cooldown` before being let back in. Entmoot's take on HiveMQ Data
//! Hub's behavior policies (see ENTERPRISE_ROADMAP.md): a client lifecycle
//! state machine, evaluated locally per node with no cross-node
//! coordination, same as everything else in that policy family.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Admit,
    Quarantined,
}

#[derive(Default)]
struct ClientHistory {
    connects: VecDeque<Instant>,
    quarantined_until: Option<Instant>,
}

pub struct ChurnGuard {
    max_reconnects: u32,
    window: Duration,
    cooldown: Duration,
    clients: Mutex<HashMap<String, ClientHistory>>,
}

impl ChurnGuard {
    /// `max_reconnects = 0` disables churn quarantine entirely (default).
    pub fn new(max_reconnects: u32, window_secs: u64, cooldown_secs: u64) -> Self {
        Self {
            max_reconnects,
            window: Duration::from_secs(window_secs.max(1)),
            cooldown: Duration::from_secs(cooldown_secs.max(1)),
            clients: Mutex::new(HashMap::new()),
        }
    }

    /// Record a connect attempt for `client_id` and say whether it's
    /// quarantined.
    pub fn admit(&self, client_id: &str) -> Verdict {
        if self.max_reconnects == 0 {
            return Verdict::Admit;
        }
        let now = Instant::now();
        let mut map = self.clients.lock().unwrap();
        let entry = map.entry(client_id.to_string()).or_default();

        if let Some(until) = entry.quarantined_until {
            if now < until {
                return Verdict::Quarantined;
            }
            entry.quarantined_until = None;
            entry.connects.clear();
        }

        while entry.connects.front().is_some_and(|t| now.duration_since(*t) >= self.window) {
            entry.connects.pop_front();
        }
        entry.connects.push_back(now);

        if entry.connects.len() as u32 > self.max_reconnects {
            entry.quarantined_until = Some(now + self.cooldown);
            Verdict::Quarantined
        } else {
            Verdict::Admit
        }
    }

    /// Drop history for clients that are neither currently quarantined nor
    /// have connected within `idle_after`, so the map doesn't grow
    /// unboundedly with one-off client ids.
    pub fn sweep(&self, idle_after: Duration) {
        let now = Instant::now();
        self.clients.lock().unwrap().retain(|_, h| match h.quarantined_until {
            Some(until) => now < until,
            None => h.connects.back().is_some_and(|t| now.duration_since(*t) < idle_after),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn disabled_admits_unconditionally() {
        let g = ChurnGuard::new(0, 60, 60);
        for _ in 0..20 {
            assert_eq!(g.admit("client-a"), Verdict::Admit);
        }
    }

    #[test]
    fn quarantines_after_exceeding_max_reconnects_within_window() {
        let g = ChurnGuard::new(3, 60, 60);
        for _ in 0..3 {
            assert_eq!(g.admit("client-a"), Verdict::Admit);
        }
        assert_eq!(g.admit("client-a"), Verdict::Quarantined, "4th connect within the window should be caught");
        // Stays quarantined on subsequent attempts too, not just the one
        // that tripped it.
        assert_eq!(g.admit("client-a"), Verdict::Quarantined);
    }

    #[test]
    fn other_clients_are_unaffected() {
        let g = ChurnGuard::new(1, 60, 60);
        assert_eq!(g.admit("client-a"), Verdict::Admit);
        assert_eq!(g.admit("client-a"), Verdict::Quarantined);
        assert_eq!(g.admit("client-b"), Verdict::Admit, "a different client id must not share client-a's history");
    }

    #[test]
    fn window_expiry_resets_the_count() {
        let g = ChurnGuard::new(1, 0, 60); // window floors to 1s in `new`... see below
        // Use a real (short) window instead of relying on the 1s floor, so
        // the test doesn't need to sleep a full second.
        let g = ChurnGuard { window: Duration::from_millis(80), ..g };
        assert_eq!(g.admit("client-a"), Verdict::Admit);
        sleep(Duration::from_millis(120)); // past the window
        assert_eq!(g.admit("client-a"), Verdict::Admit, "the earlier connect should have aged out of the window");
    }

    #[test]
    fn quarantine_expires_after_cooldown() {
        let g = ChurnGuard::new(1, 60, 60);
        let g = ChurnGuard { cooldown: Duration::from_millis(100), ..g };
        assert_eq!(g.admit("client-a"), Verdict::Admit);
        assert_eq!(g.admit("client-a"), Verdict::Quarantined);
        sleep(Duration::from_millis(150));
        assert_eq!(g.admit("client-a"), Verdict::Admit, "quarantine should have expired");
    }

    #[test]
    fn sweep_drops_idle_non_quarantined_clients() {
        let g = ChurnGuard::new(5, 60, 60);
        g.admit("client-a");
        assert_eq!(g.clients.lock().unwrap().len(), 1);
        g.sweep(Duration::from_millis(10));
        assert_eq!(g.clients.lock().unwrap().len(), 1, "not idle yet");
        sleep(Duration::from_millis(20));
        g.sweep(Duration::from_millis(10));
        assert_eq!(g.clients.lock().unwrap().len(), 0, "idle entry should be swept");
    }

    #[test]
    fn sweep_keeps_active_quarantine() {
        let g = ChurnGuard::new(1, 60, 60);
        let g = ChurnGuard { cooldown: Duration::from_secs(60), ..g };
        g.admit("client-a");
        g.admit("client-a"); // now quarantined for 60s
        g.sweep(Duration::from_millis(1)); // would drop on idle alone
        assert_eq!(g.clients.lock().unwrap().len(), 1, "an active quarantine must survive a sweep");
    }
}
