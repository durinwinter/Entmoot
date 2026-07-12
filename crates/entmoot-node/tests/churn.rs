//! Reconnect-churn quarantine (Entmoot's take on HiveMQ Data Hub behavior
//! policies, see ENTERPRISE_ROADMAP.md): a specific client id reconnecting
//! too often within a window gets quarantined for a cooldown, independent
//! of workstream 1's aggregate connect-admission control.

use entmoot_core::NodeConfig;
use rumqttc::{ConnectionError, MqttOptions};
use std::time::Duration;
use tokio::time::timeout;

fn node_cfg(id: &str, mqtt_port: u16, zenoh_port: u16) -> NodeConfig {
    NodeConfig {
        id: id.into(),
        mqtt_listen: format!("127.0.0.1:{mqtt_port}"),
        zenoh_listen: vec![format!("tcp/127.0.0.1:{zenoh_port}")],
        peers: vec![],
        scope: "churn-test".into(),
        ..NodeConfig::default()
    }
}

fn client_opts(name: &str, port: u16) -> MqttOptions {
    let mut opts = MqttOptions::new(name, "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(5));
    opts
}

async fn connect_once(port: u16) -> Result<(), rumqttc::ConnectReturnCode> {
    let (_client, mut events) = rumqttc::AsyncClient::new(client_opts("flapper", port), 16);
    match timeout(Duration::from_secs(5), events.poll()).await.unwrap() {
        Ok(_) => Ok(()),
        Err(ConnectionError::ConnectionRefused(code)) => Err(code),
        other => panic!("unexpected connect outcome: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn flapping_client_is_quarantined_then_recovers_after_cooldown() {
    let mut cfg = node_cfg("churn-node", 18921, 17551);
    cfg.churn_max_reconnects = 2;
    cfg.churn_window_secs = 60;
    cfg.churn_cooldown_secs = 1;
    let node = entmoot_node::run(cfg).await.unwrap();

    // First two connects (within max_reconnects) succeed.
    assert!(connect_once(18921).await.is_ok(), "1st connect should be admitted");
    assert!(connect_once(18921).await.is_ok(), "2nd connect should be admitted");
    // The 3rd trips the quarantine.
    assert_eq!(
        connect_once(18921).await,
        Err(rumqttc::ConnectReturnCode::ServiceUnavailable),
        "3rd reconnect within the window should be quarantined"
    );
    // Stays quarantined immediately after, not just on the triggering attempt.
    assert_eq!(connect_once(18921).await, Err(rumqttc::ConnectReturnCode::ServiceUnavailable));

    assert_eq!(
        node.broker.metrics.churn_quarantined_total.load(std::sync::atomic::Ordering::Relaxed),
        2,
        "two connects should have been caught by the quarantine"
    );

    tokio::time::sleep(Duration::from_millis(1200)).await; // past the 1s cooldown
    assert!(connect_once(18921).await.is_ok(), "connect should succeed again once the cooldown expires");

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_different_client_id_is_unaffected() {
    let mut cfg = node_cfg("churn-node-2", 18922, 17552);
    cfg.churn_max_reconnects = 1;
    cfg.churn_window_secs = 60;
    cfg.churn_cooldown_secs = 60;
    let node = entmoot_node::run(cfg).await.unwrap();

    let (_c, mut e) = rumqttc::AsyncClient::new(client_opts("client-a", 18922), 16);
    timeout(Duration::from_secs(5), e.poll()).await.unwrap().unwrap();
    let (_c, mut e) = rumqttc::AsyncClient::new(client_opts("client-a", 18922), 16);
    let refused = timeout(Duration::from_secs(5), e.poll()).await.unwrap();
    assert!(matches!(refused, Err(ConnectionError::ConnectionRefused(rumqttc::ConnectReturnCode::ServiceUnavailable))));

    // A different client id must not share client-a's quarantine.
    let (_c, mut e) = rumqttc::AsyncClient::new(client_opts("client-b", 18922), 16);
    timeout(Duration::from_secs(5), e.poll()).await.unwrap().unwrap();

    node.shutdown().await;
}
