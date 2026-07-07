//! Authentication (who are you) and topic authorization (what may you touch).

use crate::config::{AclRule, AuthConfig, Policy};
use crate::topic;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

pub fn sha256_hex(s: &str) -> String {
    let digest = Sha256::digest(s.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectDenied {
    BadCredentials,
    AnonymousNotAllowed,
}

pub struct Authenticator {
    users: HashMap<String, String>,
    allow_anonymous: bool,
}

impl Authenticator {
    pub fn new(cfg: &AuthConfig) -> Self {
        Self {
            users: cfg
                .users
                .iter()
                .map(|u| (u.name.clone(), u.password_sha256.to_lowercase()))
                .collect(),
            allow_anonymous: cfg.allow_anonymous,
        }
    }

    /// Returns the authenticated identity; the empty string is "anonymous".
    pub fn authenticate(&self, login: Option<(&str, &str)>) -> Result<String, ConnectDenied> {
        match login {
            Some((user, password)) if !user.is_empty() => match self.users.get(user) {
                Some(stored) if *stored == sha256_hex(password) => Ok(user.to_string()),
                _ => Err(ConnectDenied::BadCredentials),
            },
            _ if self.allow_anonymous => Ok(String::new()),
            _ => Err(ConnectDenied::AnonymousNotAllowed),
        }
    }
}

pub struct Acl {
    rules: Vec<AclRule>,
    default_allow: bool,
}

impl Acl {
    pub fn new(rules: Vec<AclRule>, policy: Policy) -> Self {
        Self { rules, default_allow: policy == Policy::Allow }
    }

    fn rules_for<'a>(&'a self, user: &'a str) -> impl Iterator<Item = &'a AclRule> {
        self.rules.iter().filter(move |r| r.user == "*" || r.user == user)
    }

    pub fn may_publish(&self, user: &str, topic_name: &str) -> bool {
        self.default_allow
            || self
                .rules_for(user)
                .any(|r| r.publish.iter().any(|pat| topic::topic_matches(pat, topic_name)))
    }

    pub fn may_subscribe(&self, user: &str, filter: &str) -> bool {
        self.default_allow
            || self
                .rules_for(user)
                .any(|r| r.subscribe.iter().any(|pat| topic::filter_covers(pat, filter)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::UserCred;

    fn auth(allow_anon: bool) -> Authenticator {
        Authenticator::new(&AuthConfig {
            allow_anonymous: allow_anon,
            default_policy: Policy::Deny,
            users: vec![UserCred {
                name: "plc1".into(),
                password_sha256: sha256_hex("secret"),
            }],
        })
    }

    #[test]
    fn password_auth() {
        let a = auth(false);
        assert_eq!(a.authenticate(Some(("plc1", "secret"))), Ok("plc1".into()));
        assert_eq!(
            a.authenticate(Some(("plc1", "wrong"))),
            Err(ConnectDenied::BadCredentials)
        );
        assert_eq!(
            a.authenticate(Some(("ghost", "secret"))),
            Err(ConnectDenied::BadCredentials)
        );
        assert_eq!(a.authenticate(None), Err(ConnectDenied::AnonymousNotAllowed));
        assert_eq!(auth(true).authenticate(None), Ok(String::new()));
    }

    #[test]
    fn acl_deny_by_default() {
        let acl = Acl::new(
            vec![AclRule {
                user: "plc1".into(),
                publish: vec!["plant/#".into()],
                subscribe: vec!["cmd/plc1/#".into()],
            }],
            Policy::Deny,
        );
        assert!(acl.may_publish("plc1", "plant/kiln1/temp"));
        assert!(!acl.may_publish("plc1", "cmd/plc2/reboot"));
        assert!(!acl.may_publish("other", "plant/kiln1/temp"));
        assert!(acl.may_subscribe("plc1", "cmd/plc1/+"));
        assert!(!acl.may_subscribe("plc1", "cmd/#"));
        assert!(!acl.may_subscribe("", "plant/#"));
    }

    #[test]
    fn acl_wildcard_user_and_allow_policy() {
        let acl = Acl::new(
            vec![AclRule {
                user: "*".into(),
                publish: vec![],
                subscribe: vec!["status/#".into()],
            }],
            Policy::Deny,
        );
        assert!(acl.may_subscribe("", "status/+"));
        assert!(!acl.may_publish("", "status/x"));

        let open = Acl::new(vec![], Policy::Allow);
        assert!(open.may_publish("", "anything/at/all"));
        assert!(open.may_subscribe("", "#"));
    }
}
