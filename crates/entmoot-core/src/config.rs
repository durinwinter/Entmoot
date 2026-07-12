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
    /// MQTT-over-TLS listener; absent = plain MQTT only.
    pub tls: Option<TlsConfig>,
    pub auth: AuthConfig,
    /// Grants consulted when `auth.default_policy = "deny"`.
    pub acl: Vec<AclRule>,
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
            tls: None,
            auth: AuthConfig::default(),
            acl: Vec::new(),
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
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self { allow_anonymous: true, default_policy: Policy::Allow, users: Vec::new() }
    }
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
