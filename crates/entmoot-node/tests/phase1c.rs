//! Phase 1c acceptance tests: persisted subscription metadata survives a
//! node restart and is reinstated *before* any client reconnects, so an
//! offline persistent session resumes collecting messages immediately - not
//! only after the device itself comes back - and a revoked ACL grant is
//! honored rather than silently reinstated from stale disk state.

use entmoot_core::config::{AclRule, AuthConfig, Policy};
use entmoot_core::NodeConfig;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;
use tokio::time::timeout;

fn node_cfg(id: &str, mqtt_port: u16, zenoh_port: u16) -> NodeConfig {
    NodeConfig {
        id: id.into(),
        mqtt_listen: format!("127.0.0.1:{mqtt_port}"),
        zenoh_listen: vec![format!("tcp/127.0.0.1:{zenoh_port}")],
        scope: "p1c-test".into(),
        ..NodeConfig::default()
    }
}

fn client_opts(name: &str, port: u16) -> MqttOptions {
    let mut opts = MqttOptions::new(name, "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(5));
    opts
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

async fn await_suback(events: &mut rumqttc::EventLoop) -> rumqttc::SubAck {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Event::Incoming(Packet::SubAck(a)) = events.poll().await.unwrap() {
                return a;
            }
        }
    })
    .await
    .expect("no SUBACK")
}

async fn await_connack(events: &mut rumqttc::EventLoop) -> rumqttc::ConnAck {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Event::Incoming(Packet::ConnAck(c)) = events.poll().await.unwrap() {
                return c;
            }
        }
    })
    .await
    .expect("no CONNACK")
}

#[tokio::test(flavor = "multi_thread")]
async fn subscription_rehydrates_after_restart() {
    let dir = std::env::temp_dir().join(format!("entmoot-p1c-subs-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let mut cfg = node_cfg("sr-1", 18871, 17501);
    cfg.data_dir = Some(dir.to_string_lossy().into_owned());
    let node = entmoot_node::run(cfg).await.unwrap();

    let mut opts = client_opts("sr-plc", 18871);
    opts.set_clean_session(false);
    let (client, mut events) = AsyncClient::new(opts, 16);
    client.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut events).await;

    // Device goes away and the whole node restarts - a fresh process, no
    // in-memory state survives except what was persisted to `data_dir`.
    drop(client);
    drop(events);
    tokio::time::sleep(Duration::from_millis(300)).await;
    node.shutdown().await;

    let mut cfg = node_cfg("sr-2", 18872, 17502);
    cfg.data_dir = Some(dir.to_string_lossy().into_owned());
    let node = entmoot_node::run(cfg).await.unwrap();

    // A message published now - while the device is still offline, after the
    // restart - must be captured by the rehydrated subscription and queued.
    // Without rehydration nothing would be subscribing on the mesh for this
    // client id yet, and the message would simply be lost.
    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("sr-feeder", 18872), 16);
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });
    pub_client
        .publish("plant/kiln1/temp", QoS::AtLeastOnce, false, "post-restart")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut opts = client_opts("sr-plc", 18872);
    opts.set_clean_session(false);
    let (_client, mut events) = AsyncClient::new(opts, 16);
    let connack = await_connack(&mut events).await;
    assert!(connack.session_present, "rehydrated session must resume");

    let p = await_publish(&mut events).await;
    assert_eq!(p.topic, "plant/kiln1/temp");
    assert_eq!(&p.payload[..], b"post-restart");

    node.shutdown().await;
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread")]
async fn revoked_acl_drops_persisted_subscription_on_rehydrate() {
    let dir = std::env::temp_dir().join(format!("entmoot-p1c-acl-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let mut cfg = node_cfg("ra-1", 18873, 17503);
    cfg.data_dir = Some(dir.to_string_lossy().into_owned());
    cfg.auth = AuthConfig { allow_anonymous: true, default_policy: Policy::Deny, users: vec![] };
    cfg.acl = vec![AclRule {
        user: "*".into(),
        publish: vec!["plant/#".into()],
        subscribe: vec!["plant/#".into()],
    }];
    let node = entmoot_node::run(cfg).await.unwrap();

    let mut opts = client_opts("ra-plc", 18873);
    opts.set_clean_session(false);
    let (client, mut events) = AsyncClient::new(opts, 16);
    client.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut events).await;
    drop(client);
    drop(events);
    tokio::time::sleep(Duration::from_millis(300)).await;
    node.shutdown().await;

    // Restart with the subscribe grant removed (publish stays allowed so we
    // can still prove the message crosses the mesh but isn't captured).
    let mut cfg = node_cfg("ra-2", 18874, 17504);
    cfg.data_dir = Some(dir.to_string_lossy().into_owned());
    cfg.auth = AuthConfig { allow_anonymous: true, default_policy: Policy::Deny, users: vec![] };
    cfg.acl = vec![AclRule { user: "*".into(), publish: vec!["plant/#".into()], subscribe: vec![] }];
    let node = entmoot_node::run(cfg).await.unwrap();

    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("ra-feeder", 18874), 16);
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });
    pub_client
        .publish("plant/kiln1/temp", QoS::AtLeastOnce, false, "should-not-be-queued")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The device reconnects: the server still holds session state (so
    // session_present is true), but nothing was queued for the dropped
    // subscription.
    let mut opts = client_opts("ra-plc", 18874);
    opts.set_clean_session(false);
    let (_client, mut events) = AsyncClient::new(opts, 16);
    let connack = await_connack(&mut events).await;
    assert!(connack.session_present);

    let leaked = timeout(Duration::from_millis(1500), await_publish(&mut events)).await;
    assert!(leaked.is_err(), "revoked subscription still delivered a message: {leaked:?}");

    node.shutdown().await;
    std::fs::remove_dir_all(&dir).ok();
}
