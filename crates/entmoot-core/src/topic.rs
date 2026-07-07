//! MQTT topic <-> Zenoh key-expression mapping.
//!
//! MQTT `+` maps to Zenoh `*`, MQTT `#` to Zenoh `**`. Beyond the MQTT spec we
//! reject empty levels, leading/trailing `/`, and the characters `* $ ? #`
//! inside names, because those are Zenoh key-expression syntax and an
//! industrial namespace has no business using them anyway. The one blessed
//! `$` name is a leading `$SYS` level in subscription filters (node stats).

use std::borrow::Cow;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TopicError {
    #[error("topic is empty")]
    Empty,
    #[error("topic has an empty level (leading/trailing or doubled '/')")]
    EmptyLevel,
    #[error("wildcard '{0}' is not allowed in a publish topic")]
    WildcardInTopic(char),
    #[error("'#' must be the final level of a filter")]
    HashNotLast,
    #[error("wildcard must occupy a whole level (got {0:?})")]
    PartialWildcard(String),
    #[error("character {0:?} is reserved zenoh syntax and not allowed")]
    ReservedChar(char),
}

fn check_level_chars(level: &str) -> Result<(), TopicError> {
    // A leading '@' marks a verbatim chunk in zenoh (wildcards skip it); we
    // reserve that space for internal keys like the retained-message store.
    if level.starts_with('@') {
        return Err(TopicError::ReservedChar('@'));
    }
    for c in level.chars() {
        if matches!(c, '*' | '$' | '?') {
            return Err(TopicError::ReservedChar(c));
        }
    }
    Ok(())
}

fn levels(s: &str) -> Result<Vec<&str>, TopicError> {
    if s.is_empty() {
        return Err(TopicError::Empty);
    }
    let parts: Vec<&str> = s.split('/').collect();
    if parts.iter().any(|p| p.is_empty()) {
        return Err(TopicError::EmptyLevel);
    }
    Ok(parts)
}

fn scoped(scope: &str, ke: String) -> String {
    if scope.is_empty() {
        ke
    } else {
        format!("{scope}/{ke}")
    }
}

/// Map a concrete publish topic (no wildcards allowed) to a Zenoh key expression.
pub fn topic_to_keyexpr(topic: &str, scope: &str) -> Result<String, TopicError> {
    let parts = levels(topic)?;
    for level in &parts {
        if level.contains('+') {
            return Err(TopicError::WildcardInTopic('+'));
        }
        if level.contains('#') {
            return Err(TopicError::WildcardInTopic('#'));
        }
        check_level_chars(level)?;
    }
    Ok(scoped(scope, parts.join("/")))
}

/// Map an MQTT subscription filter (may contain `+` / `#`) to a Zenoh key expression.
///
/// A filter whose first level is exactly `$SYS` maps onto the internal
/// [`SYS_CHUNK`] keyspace. Because that chunk is zenoh-verbatim, `#` and `+`
/// filters can never match it — which is exactly what MQTT-4.7.2-1 requires
/// of topics beginning with `$`.
pub fn filter_to_keyexpr(filter: &str, scope: &str) -> Result<String, TopicError> {
    let parts = levels(filter)?;
    let last = parts.len() - 1;
    let mut out = Vec::with_capacity(parts.len());
    for (i, level) in parts.iter().enumerate() {
        match *level {
            SYS_PREFIX if i == 0 => out.push(SYS_CHUNK),
            "+" => out.push("*"),
            "#" => {
                if i != last {
                    return Err(TopicError::HashNotLast);
                }
                out.push("**");
            }
            l if l.contains('+') || l.contains('#') => {
                return Err(TopicError::PartialWildcard(l.to_string()));
            }
            l => {
                check_level_chars(l)?;
                out.push(l);
            }
        }
    }
    Ok(scoped(scope, out.join("/")))
}

fn strip_scope<'a>(ke: &'a str, scope: &str) -> Option<&'a str> {
    if scope.is_empty() {
        return Some(ke);
    }
    ke.strip_prefix(scope)?.strip_prefix('/')
}

/// Map a Zenoh key expression from an incoming sample back to an MQTT topic.
/// Samples arrive on subscriptions we declared, so the keyexpr is concrete
/// (no wildcards) and starts with our scope if one is configured. Keys in the
/// sys keyspace surface as `$SYS/...`; other internal `@` keyspaces never
/// surface as client topics.
pub fn keyexpr_to_topic<'a>(ke: &'a str, scope: &str) -> Option<Cow<'a, str>> {
    let rest = strip_scope(ke, scope)?;
    if let Some(sys) = rest.strip_prefix(SYS_CHUNK) {
        let sys = sys.strip_prefix('/')?;
        return Some(Cow::Owned(format!("{SYS_PREFIX}/{sys}")));
    }
    if rest.starts_with('@') {
        return None;
    }
    Some(Cow::Borrowed(rest))
}

/// Does an MQTT filter match a concrete topic? Both are assumed validated.
pub fn topic_matches(filter: &str, topic_name: &str) -> bool {
    let f: Vec<&str> = filter.split('/').collect();
    let t: Vec<&str> = topic_name.split('/').collect();
    for (i, level) in f.iter().enumerate() {
        match *level {
            "#" => return true, // validated: always the last level; matches the parent too
            "+" => {
                if i >= t.len() {
                    return false;
                }
            }
            l => {
                if t.get(i) != Some(&l) {
                    return false;
                }
            }
        }
    }
    f.len() == t.len()
}

/// Is every topic matched by `inner` also matched by `outer`? Used for ACLs:
/// a requested subscription filter must be covered by a granted one.
/// Conservative: literals only cover identical literals.
pub fn filter_covers(outer: &str, inner: &str) -> bool {
    let o: Vec<&str> = outer.split('/').collect();
    let i: Vec<&str> = inner.split('/').collect();
    let mut idx = 0;
    loop {
        match (o.get(idx), i.get(idx)) {
            (Some(&"#"), _) => return true,
            (Some(&"+"), Some(&level)) => {
                if level == "#" {
                    return false; // '#' spans many levels; '+' grants only one
                }
            }
            (Some(&outer_level), Some(&inner_level)) => {
                if outer_level != inner_level {
                    return false;
                }
            }
            (None, None) => return true,
            _ => return false,
        }
        idx += 1;
    }
}

/// Internal keyspace for the mesh-wide retained-message store. The chunk
/// starts with '@' (zenoh verbatim), so client subscriptions — whose topic
/// levels may not start with '@' — can never observe or forge it.
pub const RETAINED_CHUNK: &str = "@retained";

/// Keyexpr under which the retained copy of `topic` is stored (topic validated).
pub fn retained_keyexpr(topic_name: &str, scope: &str) -> String {
    scoped(scope, format!("{RETAINED_CHUNK}/{topic_name}"))
}

/// Filter covering the whole retained keyspace (for the store's subscriber,
/// queryable, and startup fetch).
pub fn retained_filter(scope: &str) -> String {
    scoped(scope, format!("{RETAINED_CHUNK}/**"))
}

/// Recover the MQTT topic from a retained-keyspace keyexpr.
pub fn retained_keyexpr_to_topic<'a>(ke: &'a str, scope: &str) -> Option<&'a str> {
    strip_scope(ke, scope)?
        .strip_prefix(RETAINED_CHUNK)?
        .strip_prefix('/')
}

/// The `$SYS` topic space: node stats, subscribe-only for clients. Stored
/// under a verbatim chunk like the retained store, so it is unforgeable
/// (publish topics may not start with `$` or `@`) and invisible to `#`/`+`
/// wildcards (MQTT-4.7.2-1).
pub const SYS_PREFIX: &str = "$SYS";
pub const SYS_CHUNK: &str = "@sys";

/// Keyexpr on which the node publishes the `$SYS/<suffix>` topic.
pub fn sys_keyexpr(suffix: &str, scope: &str) -> String {
    scoped(scope, format!("{SYS_CHUNK}/{suffix}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_topics_map_verbatim() {
        assert_eq!(topic_to_keyexpr("plant/kiln1/temp", "").unwrap(), "plant/kiln1/temp");
        assert_eq!(topic_to_keyexpr("a", "").unwrap(), "a");
    }

    #[test]
    fn scope_is_prefixed_and_stripped() {
        assert_eq!(topic_to_keyexpr("a/b", "entmoot").unwrap(), "entmoot/a/b");
        assert_eq!(keyexpr_to_topic("entmoot/a/b", "entmoot").as_deref(), Some("a/b"));
        assert_eq!(keyexpr_to_topic("other/a/b", "entmoot"), None);
        assert_eq!(keyexpr_to_topic("a/b", "").as_deref(), Some("a/b"));
    }

    #[test]
    fn wildcards_translate() {
        assert_eq!(filter_to_keyexpr("plant/+/temp", "").unwrap(), "plant/*/temp");
        assert_eq!(filter_to_keyexpr("plant/#", "").unwrap(), "plant/**");
        assert_eq!(filter_to_keyexpr("#", "").unwrap(), "**");
        assert_eq!(filter_to_keyexpr("+", "").unwrap(), "*");
    }

    #[test]
    fn invalid_shapes_are_rejected() {
        assert_eq!(topic_to_keyexpr("", ""), Err(TopicError::Empty));
        assert_eq!(topic_to_keyexpr("a//b", ""), Err(TopicError::EmptyLevel));
        assert_eq!(topic_to_keyexpr("/a", ""), Err(TopicError::EmptyLevel));
        assert_eq!(topic_to_keyexpr("a/", ""), Err(TopicError::EmptyLevel));
        assert_eq!(topic_to_keyexpr("a/+/b", ""), Err(TopicError::WildcardInTopic('+')));
        assert_eq!(topic_to_keyexpr("a/#", ""), Err(TopicError::WildcardInTopic('#')));
        assert_eq!(filter_to_keyexpr("a/#/b", ""), Err(TopicError::HashNotLast));
        assert_eq!(
            filter_to_keyexpr("a/b+/c", ""),
            Err(TopicError::PartialWildcard("b+".into()))
        );
        assert_eq!(topic_to_keyexpr("a/b*c", ""), Err(TopicError::ReservedChar('*')));
        assert_eq!(topic_to_keyexpr("$SYS/x", ""), Err(TopicError::ReservedChar('$')));
        assert_eq!(filter_to_keyexpr("a/?", ""), Err(TopicError::ReservedChar('?')));
        assert_eq!(topic_to_keyexpr("a/@retained/b", ""), Err(TopicError::ReservedChar('@')));
        assert_eq!(filter_to_keyexpr("@retained/#", ""), Err(TopicError::ReservedChar('@')));
    }

    #[test]
    fn matching() {
        assert!(topic_matches("plant/+/temp", "plant/kiln1/temp"));
        assert!(topic_matches("plant/#", "plant/kiln1/temp"));
        assert!(topic_matches("plant/#", "plant"));
        assert!(topic_matches("#", "a/b/c"));
        assert!(topic_matches("a/b", "a/b"));
        assert!(!topic_matches("plant/+/temp", "plant/kiln1/pressure"));
        assert!(!topic_matches("plant/+", "plant/kiln1/temp"));
        assert!(!topic_matches("a/b", "a"));
        assert!(!topic_matches("+", "a/b"));
    }

    #[test]
    fn coverage() {
        assert!(filter_covers("#", "anything/goes"));
        assert!(filter_covers("plant/#", "plant/+/temp"));
        assert!(filter_covers("plant/#", "plant"));
        assert!(filter_covers("plant/+/temp", "plant/kiln1/temp"));
        assert!(filter_covers("a/b", "a/b"));
        assert!(!filter_covers("plant/+", "plant/#"));
        assert!(!filter_covers("plant/kiln1/#", "plant/#"));
        assert!(!filter_covers("a/b", "a/+"));
        assert!(!filter_covers("a/b", "a/b/c"));
    }

    #[test]
    fn retained_keyspace() {
        assert_eq!(retained_keyexpr("a/b", "scope"), "scope/@retained/a/b");
        assert_eq!(retained_filter(""), "@retained/**");
        assert_eq!(retained_keyexpr_to_topic("scope/@retained/a/b", "scope"), Some("a/b"));
        assert_eq!(retained_keyexpr_to_topic("scope/a/b", "scope"), None);
        // Internal keyspaces never surface as client topics.
        assert_eq!(keyexpr_to_topic("scope/@retained/a/b", "scope"), None);
    }

    #[test]
    fn sys_keyspace() {
        // Subscribable, with wildcards below the $SYS root.
        assert_eq!(filter_to_keyexpr("$SYS/#", "").unwrap(), "@sys/**");
        assert_eq!(filter_to_keyexpr("$SYS/broker/+/uptime", "s").unwrap(), "s/@sys/broker/*/uptime");
        // But never publishable or forgeable, and only at the first level.
        assert_eq!(topic_to_keyexpr("$SYS/broker/x", ""), Err(TopicError::ReservedChar('$')));
        assert_eq!(filter_to_keyexpr("a/$SYS/#", ""), Err(TopicError::ReservedChar('$')));
        // Samples map back to $SYS topics.
        assert_eq!(
            keyexpr_to_topic("s/@sys/broker/n1/uptime", "s").as_deref(),
            Some("$SYS/broker/n1/uptime")
        );
        assert_eq!(sys_keyexpr("broker/n1/uptime", "s"), "s/@sys/broker/n1/uptime");
        assert_eq!(keyexpr_to_topic("s/@sys", "s"), None);
    }
}
