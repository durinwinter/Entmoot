//! Per-identity connection quotas (ENTERPRISE_ROADMAP.md "multi-tenancy and
//! quotas"): a `[[quota]]` rule caps how many concurrent connections a
//! single identity may hold, independent of client id and independent of
//! the node-wide `max_connections` ceiling, so one tenant/identity can't
//! exhaust the node for everyone else sharing it.

use entmoot_core::auth::sha256_hex;
use entmoot_core::config::{AuthConfig, Policy, QuotaRule, UserCred};
use entmoot_core::NodeConfig;
use rumqttc::{AsyncClient, ConnectionError, MqttOptions};
use std::time::Duration;
use tokio::time::timeout;

fn node_cfg(id: &str, mqtt_port: u16, zenoh_port: u16, quota: Vec<QuotaRule>) -> NodeConfig {
    NodeConfig {
        id: id.into(),
        mqtt_listen: format!("127.0.0.1:{mqtt_port}"),
        zenoh_listen: vec![format!("tcp/127.0.0.1:{zenoh_port}")],
        peers: vec![],
        scope: "quota-test".into(),
        auth: AuthConfig {
            allow_anonymous: false,
            default_policy: Policy::Allow,
            users: vec![
                UserCred { name: "plc1".into(), password_sha256: sha256_hex("secret1") },
                UserCred { name: "plc2".into(), password_sha256: sha256_hex("secret2") },
            ],
            jwt: None,
        },
        quota,
        ..NodeConfig::default()
    }
}

fn client_opts(client_id: &str, user: &str, password: &str, port: u16) -> MqttOptions {
    let mut opts = MqttOptions::new(client_id, "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(5));
    opts.set_credentials(user, password);
    opts
}

/// Connects and drives the event loop in the background so the connection
/// stays live (keepalives, CONNACK processing) without needing to poll it
/// manually from the test body. Returns the client so it can be dropped
/// (closing the connection) when the test wants to free its quota slot.
async fn connect_and_hold(client_id: &str, user: &str, password: &str, port: u16) -> Result<AsyncClient, rumqttc::ConnectReturnCode> {
    let (client, mut events) = AsyncClient::new(client_opts(client_id, user, password, port), 16);
    match timeout(Duration::from_secs(5), events.poll()).await.unwrap() {
        Ok(_) => {
            tokio::spawn(async move { while events.poll().await.is_ok() {} });
            Ok(client)
        }
        Err(ConnectionError::ConnectionRefused(code)) => Err(code),
        other => panic!("unexpected connect outcome: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_connection_under_the_same_identity_is_refused_once_at_the_cap() {
    let cfg = node_cfg(
        "quota-a",
        18931,
        17561,
        vec![QuotaRule { user: "plc1".into(), max_connections: 1 }],
    );
    let node = entmoot_node::run(cfg).await.unwrap();

    // Different client ids, same identity: the quota is keyed on identity,
    // not client id.
    let _first = connect_and_hold("dev-a", "plc1", "secret1", 18931)
        .await
        .expect("1st connection under the quota should be admitted");

    let refused = connect_and_hold("dev-b", "plc1", "secret1", 18931).await;
    assert_eq!(
        refused.err(),
        Some(rumqttc::ConnectReturnCode::ServiceUnavailable),
        "2nd concurrent connection under the same identity should be refused"
    );
    assert_eq!(
        node.broker.metrics.quota_refused_total.load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnecting_frees_the_quota_slot_for_the_same_identity() {
    let cfg = node_cfg(
        "quota-b",
        18932,
        17562,
        vec![QuotaRule { user: "plc1".into(), max_connections: 1 }],
    );
    let node = entmoot_node::run(cfg).await.unwrap();

    let first = connect_and_hold("dev-a", "plc1", "secret1", 18932).await.unwrap();
    assert_eq!(
        connect_and_hold("dev-b", "plc1", "secret1", 18932).await.err(),
        Some(rumqttc::ConnectReturnCode::ServiceUnavailable)
    );

    first.disconnect().await.unwrap();
    // Give the node a moment to process the DISCONNECT and drop the guard
    // that releases the quota slot.
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(
        connect_and_hold("dev-b", "plc1", "secret1", 18932).await.is_ok(),
        "the freed slot should admit a new connection under the same identity"
    );

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_different_identity_is_unaffected_by_another_identitys_quota() {
    let cfg = node_cfg(
        "quota-c",
        18933,
        17563,
        vec![QuotaRule { user: "plc1".into(), max_connections: 1 }],
    );
    let node = entmoot_node::run(cfg).await.unwrap();

    let _first = connect_and_hold("dev-a", "plc1", "secret1", 18933).await.unwrap();
    assert_eq!(
        connect_and_hold("dev-b", "plc1", "secret1", 18933).await.err(),
        Some(rumqttc::ConnectReturnCode::ServiceUnavailable)
    );

    // plc2 has no quota rule of its own and no "*" fallback here, so it's
    // unaffected by plc1 being at its cap.
    assert!(connect_and_hold("dev-c", "plc2", "secret2", 18933).await.is_ok());

    node.shutdown().await;
}

#[test]
fn quota_config_key_round_trips_through_toml() {
    let cfg: NodeConfig = toml::from_str(
        r#"
id = "quota-toml"
mqtt_listen = "127.0.0.1:1883"

[[quota]]
user = "plc1"
max_connections = 50

[[quota]]
user = "*"
max_connections = 5
"#,
    )
    .unwrap();

    assert_eq!(cfg.quota.len(), 2);
    assert_eq!(cfg.quota[0].user, "plc1");
    assert_eq!(cfg.quota[0].max_connections, 50);
    assert_eq!(cfg.quota[1].user, "*");
    assert_eq!(cfg.quota[1].max_connections, 5);
}
