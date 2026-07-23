//! Per-identity connection quotas (ENTERPRISE_ROADMAP.md "multi-tenancy and
//! quotas"). `NodeConfig::max_connections` is a hard ceiling for the whole
//! node, independent of who's connecting — on a node shared by several
//! identities (tenants), that alone lets one noisy or misconfigured
//! identity consume every slot and starve the rest. A `[[quota]]` rule caps
//! a specific identity's own concurrent connections well below the
//! node-wide ceiling, so no single tenant can exhaust it.
//!
//! This is scoped by *identity* (the authenticated username or certificate
//! CN), not by `--scope`: `--scope` is a bus-namespace prefix fixed for the
//! whole node process — there is exactly one scope per running node, never
//! several multiplexed on one node — so keying a quota by scope would be
//! indistinguishable from the existing global `max_connections` and
//! wouldn't isolate tenants at all. Identity is the actual multi-tenant
//! axis within a single node, the same one `AclRule` already keys on.
//!
//! Purely local per-node policy lookup, same no-cross-node-coordination
//! shape as `staleness.rs`/`schema.rs`: a mesh-wide "tenant X gets N
//! connections total across the whole mesh" quota would need real
//! coordination and isn't attempted here (see ENTERPRISE_ROADMAP.md). The
//! live per-identity connection *counting* this policy is checked against
//! is runtime state, not policy, so it lives in `entmoot-node` instead
//! (mirroring how `entmoot-core::auth::Acl` is pure lookup while
//! `entmoot-node`'s connection handling holds the actual session state).

use crate::config::QuotaRule;

pub struct QuotaPolicy {
    rules: Vec<QuotaRule>,
}

impl QuotaPolicy {
    pub fn new(rules: Vec<QuotaRule>) -> Self {
        Self { rules }
    }

    /// Connection cap for `identity`, if any rule applies: the exact
    /// identity wins over a "*" wildcard, same precedence as `Acl`. `None`
    /// means no per-identity cap — only the node-wide `max_connections`
    /// ceiling applies. A matching rule with `max_connections = 0` also
    /// means unlimited, so a `[[quota]]` block need not repeat the default
    /// for every identity it doesn't want to cap.
    pub fn max_connections_for(&self, identity: &str) -> Option<usize> {
        let rule = self
            .rules
            .iter()
            .find(|r| r.user == identity)
            .or_else(|| self.rules.iter().find(|r| r.user == "*"))?;
        (rule.max_connections > 0).then_some(rule.max_connections)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_rules_means_unlimited() {
        let p = QuotaPolicy::new(vec![]);
        assert_eq!(p.max_connections_for("plc1"), None);
    }

    #[test]
    fn exact_identity_rule_wins_over_wildcard() {
        let p = QuotaPolicy::new(vec![
            QuotaRule { user: "*".into(), max_connections: 5 },
            QuotaRule { user: "plc1".into(), max_connections: 50 },
        ]);
        assert_eq!(p.max_connections_for("plc1"), Some(50));
        assert_eq!(p.max_connections_for("plc2"), Some(5));
    }

    #[test]
    fn zero_means_unlimited_for_that_identity_even_under_a_wildcard() {
        let p = QuotaPolicy::new(vec![
            QuotaRule { user: "*".into(), max_connections: 5 },
            QuotaRule { user: "plc1".into(), max_connections: 0 },
        ]);
        assert_eq!(p.max_connections_for("plc1"), None);
        assert_eq!(p.max_connections_for("plc2"), Some(5));
    }
}
