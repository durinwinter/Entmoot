//! Per-namespace staleness policy for retained-message delivery.
//!
//! Zenoh's retained-write merge is last-writer-wins on timestamp — the merge
//! primitive already exists. What's missing is legibility: during a
//! partition heal, a node may hand a client a retained value that is
//! correct-but-old rather than current. [`StalenessPolicy`] resolves, per
//! topic, how old is too old to present without a flag.

use crate::config::StalenessRule;
use crate::topic;

pub struct StalenessPolicy {
    rules: Vec<StalenessRule>,
    default_secs: u64,
}

impl StalenessPolicy {
    pub fn new(rules: Vec<StalenessRule>, default_secs: u64) -> Self {
        Self { rules, default_secs }
    }

    /// Staleness bound in seconds for a topic; 0 = never flag as stale. The
    /// first rule (in config order) whose filter matches wins, else the
    /// node-wide default.
    pub fn bound_secs(&self, topic_name: &str) -> u64 {
        self.rules
            .iter()
            .find(|r| topic::topic_matches(&r.filter, topic_name))
            .map(|r| r.bound_secs)
            .unwrap_or(self.default_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn falls_back_to_default_when_no_rule_matches() {
        let p = StalenessPolicy::new(vec![], 30);
        assert_eq!(p.bound_secs("plant/kiln1/temp"), 30);
    }

    #[test]
    fn first_matching_rule_wins() {
        let p = StalenessPolicy::new(
            vec![
                StalenessRule { filter: "plant/kiln1/#".into(), bound_secs: 5 },
                StalenessRule { filter: "plant/#".into(), bound_secs: 60 },
            ],
            30,
        );
        assert_eq!(p.bound_secs("plant/kiln1/temp"), 5);
        assert_eq!(p.bound_secs("plant/oven2/temp"), 60);
        assert_eq!(p.bound_secs("other/x"), 30);
    }

    #[test]
    fn zero_disables() {
        let p = StalenessPolicy::new(vec![], 0);
        assert_eq!(p.bound_secs("anything"), 0);
    }
}
