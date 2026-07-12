//! Data validation: publishes on a matching topic must conform to a JSON
//! Schema, or a configured action applies. The Entmoot-flavored slice of
//! HiveMQ's Data Governance Hub schema policies — see
//! ENTERPRISE_ROADMAP.md's HiveMQ feature-parity map for why this is the
//! most decentralization-friendly item on that whole list: validation is a
//! per-message, per-node decision with no cross-node coordination involved,
//! unlike session replication or shared subscriptions.
//!
//! Scoped to JSON Schema only for now; Protobuf schema validation is a
//! bigger lift (needs a descriptor format and a different decode path) and
//! isn't built here.

use crate::config::{SchemaFailAction, SchemaRule};
use crate::topic;
use jsonschema::Validator;

pub struct SchemaPolicy {
    rules: Vec<(String, Validator, SchemaFailAction)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// No rule matched this topic, or the rule's schema was satisfied.
    Pass,
    /// A rule matched and the publish failed it — either it wasn't valid
    /// JSON at all, or it didn't conform to the schema.
    Fail(SchemaFailAction),
}

impl SchemaPolicy {
    /// Compiles every configured schema up front so a malformed schema is a
    /// startup error, not a silent no-op discovered under load.
    pub fn new(rules: Vec<SchemaRule>) -> Result<Self, String> {
        let mut compiled = Vec::with_capacity(rules.len());
        for r in rules {
            let value: serde_json::Value = serde_json::from_str(&r.schema)
                .map_err(|e| format!("schema for filter {:?} is not valid JSON: {e}", r.filter))?;
            let validator = Validator::new(&value)
                .map_err(|e| format!("schema for filter {:?} is invalid: {e}", r.filter))?;
            compiled.push((r.filter, validator, r.on_fail));
        }
        Ok(Self { rules: compiled })
    }

    /// First matching filter (in config order) wins; topics matching no
    /// rule are unvalidated.
    pub fn check(&self, topic_name: &str, payload: &[u8]) -> Verdict {
        let Some((_, validator, action)) =
            self.rules.iter().find(|(filter, _, _)| topic::topic_matches(filter, topic_name))
        else {
            return Verdict::Pass;
        };
        match serde_json::from_slice::<serde_json::Value>(payload) {
            Ok(value) if validator.is_valid(&value) => Verdict::Pass,
            _ => Verdict::Fail(*action),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(filter: &str, schema: &str, on_fail: SchemaFailAction) -> SchemaRule {
        SchemaRule { filter: filter.into(), schema: schema.into(), on_fail }
    }

    #[test]
    fn unmatched_topic_passes() {
        let p = SchemaPolicy::new(vec![rule(
            "plant/+/temp",
            r#"{"type":"object","required":["value"]}"#,
            SchemaFailAction::Drop,
        )])
        .unwrap();
        assert_eq!(p.check("plant/kiln1/pressure", b"not even json"), Verdict::Pass);
    }

    #[test]
    fn matched_topic_enforces_schema() {
        let p = SchemaPolicy::new(vec![rule(
            "plant/+/temp",
            r#"{"type":"object","properties":{"value":{"type":"number"}},"required":["value"]}"#,
            SchemaFailAction::Drop,
        )])
        .unwrap();
        assert_eq!(p.check("plant/kiln1/temp", br#"{"value": 93.5}"#), Verdict::Pass);
        assert_eq!(
            p.check("plant/kiln1/temp", br#"{"value": "hot"}"#),
            Verdict::Fail(SchemaFailAction::Drop)
        );
        assert_eq!(p.check("plant/kiln1/temp", b"not json"), Verdict::Fail(SchemaFailAction::Drop));
    }

    #[test]
    fn first_matching_rule_wins_and_action_is_configurable() {
        let p = SchemaPolicy::new(vec![
            rule("plant/kiln1/#", r#"{"type":"object"}"#, SchemaFailAction::Disconnect),
            rule("plant/#", r#"{"type":"object"}"#, SchemaFailAction::Drop),
        ])
        .unwrap();
        assert_eq!(
            p.check("plant/kiln1/temp", b"[1,2,3]"),
            Verdict::Fail(SchemaFailAction::Disconnect)
        );
        assert_eq!(p.check("plant/oven2/temp", b"[1,2,3]"), Verdict::Fail(SchemaFailAction::Drop));
    }

    #[test]
    fn invalid_schema_is_rejected_at_construction() {
        let err = SchemaPolicy::new(vec![rule("a", r#"{"type": "not-a-real-type"}"#, SchemaFailAction::Drop)]);
        assert!(err.is_err());
    }
}
