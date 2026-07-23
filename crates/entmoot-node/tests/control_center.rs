//! Control-center-lite (ENTERPRISE_ROADMAP.md priority item 4): a client
//! attached to one node can be force-disconnected by a query issued from any
//! other node in the mesh, without either side knowing in advance which
//! node actually holds the connection.

use entmoot_core::NodeConfig;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;
use tokio::time::timeout;

fn node_cfg(id: &str, mqtt_port: u16, zenoh_port: u16, peers: Vec<String>) -> NodeConfig {
    NodeConfig {
        id: id.into(),
        mqtt_listen: format!("127.0.0.1:{mqtt_port}"),
        zenoh_listen: vec![format!("tcp/127.0.0.1:{zenoh_port}")],
        peers,
        scope: "control-center-test".into(),
        ..NodeConfig::default()
    }
}

fn client_opts(name: &str, port: u16) -> MqttOptions {
    let mut opts = MqttOptions::new(name, "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(5));
    opts
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnect_query_from_another_node_kicks_the_live_connection() {
    let node_a = entmoot_node::run(node_cfg("cc-a", 18841, 17481, vec![])).await.unwrap();
    let node_b = entmoot_node::run(node_cfg(
        "cc-b",
        18842,
        17482,
        vec!["tcp/127.0.0.1:17481".into()],
    ))
    .await
    .unwrap();

    // Give the zenoh peers a moment to establish the link (gossip needs it
    // before a broadcast query from B could ever reach A's queryable).
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (target, mut target_events) = AsyncClient::new(client_opts("cc-target", 18841), 16);
    let kicked = tokio::spawn(async move {
        loop {
            match target_events.poll().await {
                Ok(_) => continue,
                Err(_) => return, // connection dropped: this is what we're waiting for
            }
        }
    });

    // A CONNECT round trip so the session is actually attached to node A
    // before we go looking for it from node B.
    target.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let outcome = entmoot_node::ctl::disconnect_client(
        &node_b.broker.session,
        "control-center-test",
        "cc-target",
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    assert_eq!(outcome, entmoot_node::DisconnectOutcome::Kicked { node: "cc-a".into() });

    timeout(Duration::from_secs(5), kicked).await.expect("client was not disconnected").unwrap();

    node_a.shutdown().await;
    node_b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnect_query_for_an_unknown_client_finds_nothing() {
    let node = entmoot_node::run(node_cfg("cc-c", 18843, 17483, vec![])).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let outcome = entmoot_node::ctl::disconnect_client(
        &node.broker.session,
        "control-center-test",
        "no-such-client",
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert_eq!(outcome, entmoot_node::DisconnectOutcome::NotFound);

    node.shutdown().await;
}

/// A publish/subscribe/will ACL denial surfaces both as a `tracing::warn!`
/// (already covered by existing hardening tests asserting the deny behavior
/// itself) and now as a `$meta/clients` event any audit consumer can watch
/// mesh-wide, same bus as connect/disconnect (see `client_events.rs`).
#[tokio::test(flavor = "multi_thread")]
async fn acl_denials_are_visible_on_meta_clients() {
    use entmoot_core::auth::sha256_hex;
    use entmoot_core::config::{AclRule, AuthConfig, Policy, UserCred};

    let cfg = NodeConfig {
        auth: AuthConfig {
            // Anonymous is allowed only so the watcher below doesn't need
            // its own credentials; it still only gets what "*" is granted.
            allow_anonymous: true,
            default_policy: Policy::Deny,
            users: vec![UserCred { name: "plc1".into(), password_sha256: sha256_hex("secret") }],
            jwt: None,
        },
        acl: vec![
            AclRule {
                user: "plc1".into(),
                publish: vec!["plant/allowed".into()],
                subscribe: vec!["plant/allowed".into()],
            },
            AclRule { user: "*".into(), publish: vec![], subscribe: vec!["$meta/#".into()] },
        ],
        ..node_cfg("cc-audit", 18844, 17484, vec![])
    };
    let node = entmoot_node::run(cfg).await.unwrap();

    let (watcher, mut watcher_events) = AsyncClient::new(client_opts("cc-audit-watcher", 18844), 16);
    watcher.subscribe("$meta/clients/#", QoS::AtLeastOnce).await.unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            if let Event::Incoming(Packet::SubAck(_)) = watcher_events.poll().await.unwrap() {
                return;
            }
        }
    })
    .await
    .expect("no SUBACK");

    let mut opts = client_opts("plc1", 18844);
    opts.set_credentials("plc1", "secret");
    let (client, mut client_events) = AsyncClient::new(opts, 16);
    tokio::spawn(async move { while client_events.poll().await.is_ok() {} });

    client.publish("plant/forbidden", QoS::AtLeastOnce, false, "1").await.unwrap();

    let prefix = "$meta/clients/cc-audit/plc1";
    let event = timeout(Duration::from_secs(5), async {
        loop {
            if let Event::Incoming(Packet::Publish(p)) = watcher_events.poll().await.unwrap() {
                if p.topic.starts_with(prefix) {
                    let body = String::from_utf8_lossy(&p.payload).into_owned();
                    if body.starts_with("publish_denied") {
                        return body;
                    }
                }
            }
        }
    })
    .await
    .expect("no publish_denied audit event");
    assert!(event.contains("plant/forbidden"));

    node.shutdown().await;
}
