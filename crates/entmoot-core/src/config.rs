//! Node configuration: defaults, CLI overrides (in the node binary), and a
//! TOML file for the security-relevant parts (users, ACLs, TLS).

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    /// Stable node identity (shows up in logs, metrics, and $SYS topics).
    pub id: String,
    /// Plain MQTT listener, e.g. "0.0.0.0:1883".
    pub mqtt_listen: String,
    /// Entmoot bus endpoints this node listens on for peers, e.g. "tcp/0.0.0.0:7447".
    #[serde(alias = "bus_listen")]
    pub zenoh_listen: Vec<String>,
    /// Entmoot bus endpoints of peer nodes to connect to, e.g. "tcp/10.0.0.2:7447".
    pub peers: Vec<String>,
    /// Optional bus namespace prefix isolating the MQTT namespace on a shared fabric.
    pub scope: String,
    /// Maximum accepted MQTT packet size in bytes.
    pub max_packet_size: usize,
    /// Maximum concurrent MQTT connections (plain + TLS combined).
    pub max_connections: usize,
    /// Per-connection inbound PUBLISH rate limit in messages/second
    /// (burst = one second's worth). 0 disables. Violators are disconnected.
    pub max_publish_rate: u32,
    /// Directory for node-local state (currently the retained-message
    /// snapshot). Absent = memory only.
    pub data_dir: Option<String>,
    /// Prometheus text endpoint, e.g. "0.0.0.0:9464". Absent = no metrics.
    pub metrics_listen: Option<String>,
    /// Kubernetes-style health endpoint, e.g. "0.0.0.0:9465".
    /// Serves /healthz and /readyz. Absent = no health endpoint.
    pub health_listen: Option<String>,
    /// Seconds an offline persistent session (cleanSession=0) is kept before
    /// its subscriptions and queue are discarded. 0 = never expire.
    pub session_expiry_secs: u64,
    /// Maximum QoS 1 messages queued for an offline persistent session;
    /// oldest are dropped beyond this.
    pub max_queued_per_session: usize,
    /// How long a client's full outbound queue may stall delivery before the
    /// client is evicted as a slow consumer (its will fires; persistent
    /// sessions keep queueing QoS 1). 0 = never evict, block until drained.
    pub slow_consumer_grace_ms: u64,
    /// Interval for publishing node stats on `$SYS/broker/<id>/...`
    /// (subscribe-only; clients cannot publish under `$SYS`). 0 = disabled.
    pub sys_interval_secs: u64,
    /// Maximum rate (per second) at which new CONNECTs are admitted into
    /// auth/session/retained-delivery, independent of `max_connections`.
    /// Beyond this rate the CONNECT is refused with `ServiceUnavailable`
    /// instead of being processed, so a reconnect storm gets a legible
    /// backoff signal rather than silent overload. 0 = unlimited (default).
    pub connect_admission_rate: u32,
    /// Burst allowance for `connect_admission_rate`; clamped up to at least
    /// the rate itself. Ignored when the rate is 0.
    pub connect_admission_burst: u32,
    /// A client reconnecting more than this many times within
    /// `churn_window_secs` is quarantined (CONNECT refused with
    /// `ServiceUnavailable`) for `churn_cooldown_secs` — the behavior-policy
    /// counterpart to `connect_admission_rate`: that one sheds an aggregate
    /// storm, this one catches one specific client flapping. 0 = disabled
    /// (default).
    pub churn_max_reconnects: u32,
    /// Rolling window (seconds) `churn_max_reconnects` is measured over.
    pub churn_window_secs: u64,
    /// How long a flapping client is quarantined once caught.
    pub churn_cooldown_secs: u64,
    /// Default staleness bound (seconds) for retained-message delivery:
    /// during partition heal, a retained value older than this is flagged via
    /// a `$meta/<topic>` companion message instead of being silently
    /// presented as current. 0 = disabled (default).
    pub retained_staleness_secs: u64,
    /// Per-topic-filter overrides for `retained_staleness_secs`; the first
    /// matching rule (in list order) wins, else the default above applies.
    pub staleness: Vec<StalenessRule>,
    /// Caps Zenoh's own wire batch size (its MTU equivalent) in bytes —
    /// `transport/link/tx/batch_size` in Zenoh's config, max 65535. Set this
    /// below the real path MTU of a link (measure with a `ping -M do` sweep;
    /// see `scripts/mtu-sweep.sh`) to keep Zenoh from ever assembling a batch
    /// IP fragmentation would otherwise silently split, which pollutes every
    /// latency/throughput number until it's found. Absent = Zenoh's own
    /// default (65535; QUIC-datagram links additionally auto-negotiate their
    /// own MTU from the QUIC connection, so this matters most for TCP links,
    /// which don't).
    pub zenoh_link_mtu: Option<u16>,
    /// MQTT-over-TLS listener; absent = plain MQTT only.
    pub tls: Option<TlsConfig>,
    pub auth: AuthConfig,
    /// Grants consulted when `auth.default_policy = "deny"`.
    pub acl: Vec<AclRule>,
    /// Data-validation rules: publishes on a matching topic must conform to
    /// the given JSON Schema, or `on_fail` applies. First matching rule wins;
    /// topics matching none are unvalidated (as today).
    pub schema: Vec<SchemaRule>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: "entmoot-0".into(),
            mqtt_listen: "0.0.0.0:1883".into(),
            zenoh_listen: vec!["tcp/0.0.0.0:7447".into()],
            peers: Vec::new(),
            scope: String::new(),
            max_packet_size: 256 * 1024,
            max_connections: 10_000,
            max_publish_rate: 0,
            data_dir: None,
            metrics_listen: None,
            health_listen: None,
            session_expiry_secs: 24 * 60 * 60,
            max_queued_per_session: 1000,
            slow_consumer_grace_ms: 5000,
            sys_interval_secs: 10,
            connect_admission_rate: 0,
            connect_admission_burst: 0,
            churn_max_reconnects: 0,
            churn_window_secs: 60,
            churn_cooldown_secs: 300,
            retained_staleness_secs: 0,
            staleness: Vec::new(),
            zenoh_link_mtu: None,
            tls: None,
            auth: AuthConfig::default(),
            acl: Vec::new(),
            schema: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    #[serde(default = "default_tls_listen")]
    pub listen: String,
    /// PEM certificate chain.
    pub cert_file: String,
    /// PEM private key.
    pub key_file: String,
    /// PEM CA bundle for client certificates (mTLS). When set, TLS clients
    /// MUST present a certificate signed by this CA, and the certificate's
    /// Common Name becomes the client's identity for ACLs (any MQTT
    /// username/password is ignored).
    #[serde(default)]
    pub client_ca_file: Option<String>,
}

fn default_tls_listen() -> String {
    "0.0.0.0:8883".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    /// Accept clients that present no username. Default true (Phase 0 behavior);
    /// set false for a hardened deployment.
    pub allow_anonymous: bool,
    /// "allow": ACL entries are ignored, everything is permitted.
    /// "deny": only topics granted by an [[acl]] entry are permitted.
    pub default_policy: Policy,
    pub users: Vec<UserCred>,
    /// Bearer-token auth: when a CONNECT's username doesn't match a local
    /// user, its password is tried as a JWT. Additive to `users`, not a
    /// replacement — existing local-user deployments are unaffected.
    pub jwt: Option<JwtConfig>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self { allow_anonymous: true, default_policy: Policy::Allow, users: Vec::new(), jwt: None }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JwtConfig {
    /// HMAC-SHA256 shared secret used to verify token signatures. Static-key
    /// verification only (HS256) — no JWKS/OIDC discovery; if a deployment
    /// needs to trust a live identity provider's rotating keys instead of a
    /// fixed shared secret, that's a bigger, separate feature.
    pub hmac_secret: String,
    /// Claim whose value becomes the authenticated identity (for ACL
    /// matching).
    #[serde(default = "default_jwt_identity_claim")]
    pub identity_claim: String,
    /// Required `iss` claim, if any.
    #[serde(default)]
    pub issuer: Option<String>,
    /// Required `aud` claim, if any.
    #[serde(default)]
    pub audience: Option<String>,
}

fn default_jwt_identity_claim() -> String {
    "sub".into()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Policy {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserCred {
    pub name: String,
    /// Hex SHA-256 of the password; generate with `entmoot --hash-password <pw>`.
    pub password_sha256: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AclRule {
    /// Username this rule applies to; "*" matches every identity, including
    /// anonymous clients.
    pub user: String,
    /// MQTT topic filters this identity may publish to.
    pub publish: Vec<String>,
    /// MQTT topic filters this identity may subscribe to (a requested filter
    /// must be *covered* by a granted one; `plant/#` covers `plant/+/temp`).
    pub subscribe: Vec<String>,
}

impl Default for AclRule {
    fn default() -> Self {
        Self { user: "*".into(), publish: Vec::new(), subscribe: Vec::new() }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StalenessRule {
    /// MQTT topic filter (may use `+`/`#`) this bound applies to.
    pub filter: String,
    /// Seconds after which a retained value on a matching topic is flagged
    /// stale on delivery.
    pub bound_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRule {
    /// MQTT topic filter (may use `+`/`#`) this schema applies to.
    pub filter: String,
    /// Inline JSON Schema text. A publish on a matching topic must parse as
    /// JSON and validate against it.
    pub schema: String,
    /// What to do with a publish that fails validation (or isn't JSON at
    /// all). Default: drop it (acked anyway, like an ACL-denied publish —
    /// v3.1.1 has no error ack, and stalling would just make the device
    /// retry forever).
    #[serde(default)]
    pub on_fail: SchemaFailAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaFailAction {
    #[default]
    Drop,
    Disconnect,
}
