//! Authentication (who are you) and topic authorization (what may you touch).

use crate::config::{AclRule, AuthConfig, JwtConfig, Policy};
use crate::topic;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
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

/// Verifies a CONNECT password as a JWT (HS256, static shared secret) and
/// extracts the authenticated identity from a configured claim. No
/// JWKS/OIDC discovery — a fixed key, checked locally, same "stateless
/// per-connection decision" shape as everything else in this module.
struct JwtVerifier {
    key: DecodingKey,
    validation: Validation,
    identity_claim: String,
}

impl JwtVerifier {
    fn new(cfg: &JwtConfig) -> Self {
        let mut validation = Validation::new(Algorithm::HS256);
        if let Some(iss) = &cfg.issuer {
            validation.set_issuer(&[iss]);
        }
        if let Some(aud) = &cfg.audience {
            validation.set_audience(&[aud]);
        }
        Self {
            key: DecodingKey::from_secret(cfg.hmac_secret.as_bytes()),
            validation,
            identity_claim: cfg.identity_claim.clone(),
        }
    }

    /// `None` on any failure: bad signature, expired, wrong issuer/audience,
    /// or the identity claim missing/not-a-string. Deliberately doesn't
    /// distinguish *why* to the caller — same "just BadCredentials" posture
    /// as a wrong local password.
    fn verify(&self, token: &str) -> Option<String> {
        let data = decode::<serde_json::Value>(token, &self.key, &self.validation).ok()?;
        data.claims.get(&self.identity_claim)?.as_str().map(str::to_string)
    }
}

pub struct Authenticator {
    users: HashMap<String, String>,
    allow_anonymous: bool,
    jwt: Option<JwtVerifier>,
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
            jwt: cfg.jwt.as_ref().map(JwtVerifier::new),
        }
    }

    /// Returns the authenticated identity; the empty string is "anonymous".
    ///
    /// JWT is tried only when the username doesn't match a known local
    /// user — a username that *does* match must authenticate against that
    /// user's own password, not fall back to a token. Additive: with no
    /// `jwt` configured, behavior is identical to before this existed.
    pub fn authenticate(&self, login: Option<(&str, &str)>) -> Result<String, ConnectDenied> {
        match login {
            Some((user, password)) if !user.is_empty() => match self.users.get(user) {
                Some(stored) if *stored == sha256_hex(password) => Ok(user.to_string()),
                Some(_) => Err(ConnectDenied::BadCredentials),
                None => self.jwt_identity(password).ok_or(ConnectDenied::BadCredentials),
            },
            Some((_, password)) if self.jwt.is_some() && !password.is_empty() => {
                self.jwt_identity(password).ok_or(ConnectDenied::BadCredentials)
            }
            _ if self.allow_anonymous => Ok(String::new()),
            _ => Err(ConnectDenied::AnonymousNotAllowed),
        }
    }

    fn jwt_identity(&self, token: &str) -> Option<String> {
        self.jwt.as_ref()?.verify(token)
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
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;

    fn auth(allow_anon: bool) -> Authenticator {
        Authenticator::new(&AuthConfig {
            allow_anonymous: allow_anon,
            default_policy: Policy::Deny,
            users: vec![UserCred {
                name: "plc1".into(),
                password_sha256: sha256_hex("secret"),
            }],
            jwt: None,
        })
    }

    fn sign(secret: &str, claims: serde_json::Value) -> String {
        encode(&Header::new(Algorithm::HS256), &claims, &EncodingKey::from_secret(secret.as_bytes())).unwrap()
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

    fn jwt_auth() -> Authenticator {
        Authenticator::new(&AuthConfig {
            allow_anonymous: false,
            default_policy: Policy::Deny,
            users: vec![UserCred { name: "plc1".into(), password_sha256: sha256_hex("secret") }],
            jwt: Some(JwtConfig {
                hmac_secret: "test-secret".into(),
                identity_claim: "sub".into(),
                issuer: Some("entmoot-test".into()),
                audience: None,
            }),
        })
    }

    #[test]
    fn valid_jwt_authenticates_as_the_identity_claim() {
        let a = jwt_auth();
        let token = sign(
            "test-secret",
            json!({"sub": "gateway-7", "iss": "entmoot-test", "exp": 9_999_999_999u64}),
        );
        assert_eq!(a.authenticate(Some(("anything", &token))), Ok("gateway-7".into()));
        // Empty username is fine too — the token carries the identity.
        assert_eq!(a.authenticate(Some(("", &token))), Ok("gateway-7".into()));
    }

    #[test]
    fn jwt_is_only_tried_for_unknown_usernames() {
        let a = jwt_auth();
        let token = sign("test-secret", json!({"sub": "gateway-7", "iss": "entmoot-test", "exp": 9_999_999_999u64}));
        // "plc1" is a known local user: its own password must match, a
        // valid token for someone else's identity must not let it in.
        assert_eq!(a.authenticate(Some(("plc1", &token))), Err(ConnectDenied::BadCredentials));
        assert_eq!(a.authenticate(Some(("plc1", "secret"))), Ok("plc1".into()));
    }

    #[test]
    fn jwt_rejects_bad_signature_wrong_issuer_and_expired_tokens() {
        let a = jwt_auth();
        let wrong_secret = sign("nope", json!({"sub": "x", "iss": "entmoot-test", "exp": 9_999_999_999u64}));
        assert_eq!(a.authenticate(Some(("u", &wrong_secret))), Err(ConnectDenied::BadCredentials));

        let wrong_issuer = sign("test-secret", json!({"sub": "x", "iss": "someone-else", "exp": 9_999_999_999u64}));
        assert_eq!(a.authenticate(Some(("u", &wrong_issuer))), Err(ConnectDenied::BadCredentials));

        let expired = sign("test-secret", json!({"sub": "x", "iss": "entmoot-test", "exp": 1u64}));
        assert_eq!(a.authenticate(Some(("u", &expired))), Err(ConnectDenied::BadCredentials));

        let no_exp = sign("test-secret", json!({"sub": "x", "iss": "entmoot-test"}));
        assert_eq!(a.authenticate(Some(("u", &no_exp))), Err(ConnectDenied::BadCredentials));
    }

    #[test]
    fn without_jwt_configured_behavior_is_unchanged() {
        // No jwt: an unknown user is BadCredentials, not silently anonymous,
        // and an empty username with a non-empty password still falls
        // through to the plain anonymous check exactly as before this
        // feature existed.
        let a = auth(true);
        assert_eq!(a.authenticate(Some(("ghost", "whatever"))), Err(ConnectDenied::BadCredentials));
        assert_eq!(a.authenticate(Some(("", "whatever"))), Ok(String::new()));
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
