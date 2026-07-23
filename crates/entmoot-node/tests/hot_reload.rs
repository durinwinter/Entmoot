//! Config hot-reload (ENTERPRISE_ROADMAP.md "Operations Plane": hot reload
//! for config that can safely change at runtime). `Broker::reload` swaps
//! users/ACLs/schema/staleness without a restart; the SIGHUP wiring in
//! main.rs just calls it after re-reading the file, so these tests exercise
//! `reload` directly rather than sending OS signals — `cargo test` runs many
//! tests in one process, and a real SIGHUP would hit every test's signal
//! handler at once, not just the one under test.

use entmoot_core::auth::sha256_hex;
use entmoot_core::config::{AclRule, AuthConfig, Policy, SchemaFailAction, SchemaRule, UserCred};
use entmoot_core::NodeConfig;
use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions, Packet, QoS};
use std::time::Duration;
use tokio::time::timeout;

fn node_cfg(id: &str, mqtt_port: u16, zenoh_port: u16) -> NodeConfig {
    let mut cfg = NodeConfig {
        id: id.into(),
        mqtt_listen: format!("127.0.0.1:{mqtt_port}"),
        zenoh_listen: vec![format!("tcp/127.0.0.1:{zenoh_port}")],
        peers: vec![],
        scope: "reload-test".into(),
        ..NodeConfig::default()
    };
    cfg.auth = AuthConfig {
        allow_anonymous: false,
        default_policy: Policy::Deny,
        users: vec![UserCred { name: "ops".into(), password_sha256: sha256_hex("hunter2") }],
        jwt: None,
    };
    cfg.acl = vec![AclRule { user: "ops".into(), publish: vec!["plant/#".into()], subscribe: vec!["plant/#".into()] }];
    cfg
}

fn client_opts(name: &str, port: u16, user: &str, password: &str) -> MqttOptions {
    let mut opts = MqttOptions::new(name, "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(5));
    opts.set_credentials(user, password);
    opts
}

async fn connect_result(port: u16, id: &str, user: &str, password: &str) -> Result<(), rumqttc::ConnectReturnCode> {
    let (_c, mut events) = AsyncClient::new(client_opts(id, port, user, password), 16);
    match timeout(Duration::from_secs(5), events.poll()).await.unwrap() {
        Ok(_) => Ok(()),
        Err(ConnectionError::ConnectionRefused(code)) => Err(code),
        other => panic!("unexpected connect outcome: {other:?}"),
    }
}

async fn await_suback(events: &mut rumqttc::EventLoop) {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Event::Incoming(Packet::SubAck(_)) = events.poll().await.unwrap() {
                return;
            }
        }
    })
    .await
    .expect("no SUBACK")
}

async fn await_publish(events: &mut rumqttc::EventLoop) -> rumqttc::Publish {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Event::Incoming(Packet::Publish(p)) = events.poll().await.unwrap() {
                return p;
            }
        }
    })
    .await
    .expect("no publish received")
}

#[tokio::test(flavor = "multi_thread")]
async fn reload_adds_a_new_user_without_restart() {
    let node = entmoot_node::run(node_cfg("reload-user", 18941, 17571)).await.unwrap();

    assert_eq!(
        connect_result(18941, "newbie", "newuser", "newpass").await,
        Err(rumqttc::ConnectReturnCode::BadUserNamePassword),
        "newuser shouldn't exist yet"
    );

    let mut new_cfg = node_cfg("reload-user", 18941, 17571);
    new_cfg.auth.users.push(UserCred { name: "newuser".into(), password_sha256: sha256_hex("newpass") });
    node.broker.reload(&new_cfg).unwrap();

    assert!(
        connect_result(18941, "newbie2", "newuser", "newpass").await.is_ok(),
        "newuser should be able to connect immediately after reload, no restart"
    );
    // The original user must still work too.
    assert!(connect_result(18941, "ops-still", "ops", "hunter2").await.is_ok());

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reload_changes_acl_without_restart() {
    let node = entmoot_node::run(node_cfg("reload-acl", 18942, 17572)).await.unwrap();

    // "ops" starts with publish+subscribe on plant/#, revoke publish via reload.
    let mut locked_down = node_cfg("reload-acl", 18942, 17572);
    locked_down.acl = vec![AclRule { user: "ops".into(), publish: vec![], subscribe: vec!["plant/#".into()] }];
    node.broker.reload(&locked_down).unwrap();

    let (sub, mut sub_events) = AsyncClient::new(client_opts("acl-sub", 18942, "ops", "hunter2"), 16);
    sub.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut sub_events).await;

    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("acl-pub", 18942, "ops", "hunter2"), 16);
    // Drive the eventloop far enough to observe the PUBACK, so the denied
    // publish has actually reached and been processed by the server before
    // the ACL is restored below — otherwise both publishes could race ahead
    // of the network I/O and land after the restore regardless of program
    // order (a bug in test sequencing, not in reload() itself).
    pub_client.publish("plant/kiln1/temp", QoS::AtLeastOnce, false, "should-be-dropped").await.unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            if let Event::Incoming(Packet::PubAck(_)) = pub_events.poll().await.unwrap() {
                return;
            }
        }
    })
    .await
    .expect("no PUBACK for the denied publish (v3.1.1 acks ACL-denied publishes too)");

    // Restore publish permission via a second reload.
    let restored = node_cfg("reload-acl", 18942, 17572);
    node.broker.reload(&restored).unwrap();
    pub_client.publish("plant/kiln1/temp", QoS::AtLeastOnce, false, "should-succeed").await.unwrap();
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });

    let p = await_publish(&mut sub_events).await;
    assert_eq!(&p.payload[..], b"should-succeed", "only the post-restore publish should have been delivered");

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_reload_is_rejected_and_old_config_keeps_working() {
    let node = entmoot_node::run(node_cfg("reload-bad", 18943, 17573)).await.unwrap();

    let mut bad_cfg = node_cfg("reload-bad", 18943, 17573);
    bad_cfg.schema = vec![SchemaRule {
        filter: "plant/#".into(),
        schema: "{not valid json".into(),
        on_fail: SchemaFailAction::Drop,
    }];
    assert!(node.broker.reload(&bad_cfg).is_err(), "a malformed schema must be rejected, not half-applied");

    // The original, valid config must still be in effect: "ops" still works.
    assert!(connect_result(18943, "still-ops", "ops", "hunter2").await.is_ok());

    node.shutdown().await;
}
