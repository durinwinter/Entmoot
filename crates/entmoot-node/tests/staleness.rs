//! Partition merge / staleness (workstream 2 of RESILIENCE_ROADMAP.md): a
//! retained value that survives a partition is correct-but-old, not current.
//! `retained_staleness_secs` (and per-filter overrides) define how old is
//! too old; a delivery past that bound gets a `$meta/<topic>` companion
//! instead of being silently presented as fresh.
//!
//! These tests subscribe to `$meta/#`, which (workstream 6) also carries
//! client connect/subscribe/disconnect lifecycle events for every session on
//! the node, including the test's own subscriber connecting and
//! subscribing. The helpers below filter by topic rather than assuming the
//! next publish (or the absence of one) is staleness-related.

use entmoot_core::config::StalenessRule;
use entmoot_core::NodeConfig;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;
use tokio::time::timeout;

fn node_cfg(id: &str, mqtt_port: u16, zenoh_port: u16) -> NodeConfig {
    NodeConfig {
        id: id.into(),
        mqtt_listen: format!("127.0.0.1:{mqtt_port}"),
        zenoh_listen: vec![format!("tcp/127.0.0.1:{zenoh_port}")],
        peers: vec![],
        scope: "staleness-test".into(),
        ..NodeConfig::default()
    }
}

fn client_opts(name: &str, port: u16) -> MqttOptions {
    let mut opts = MqttOptions::new(name, "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(5));
    opts
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

/// Polls until a PUBLISH on exactly `topic` arrives, discarding anything else
/// (e.g. workstream-6 client lifecycle events sharing the `$meta` namespace).
async fn await_publish_on(events: &mut rumqttc::EventLoop, topic: &str) -> rumqttc::Publish {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Event::Incoming(Packet::Publish(p)) = events.poll().await.unwrap() {
                if p.topic == topic {
                    return p;
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no publish received on {topic}"))
}

/// Polls for `window`, failing if a PUBLISH on exactly `topic` arrives;
/// anything else (e.g. lifecycle events) is drained and ignored.
async fn assert_no_publish_on(events: &mut rumqttc::EventLoop, topic: &str, window: Duration) {
    let result = timeout(window, async {
        loop {
            if let Event::Incoming(Packet::Publish(p)) = events.poll().await.unwrap() {
                if p.topic == topic {
                    return p;
                }
            }
        }
    })
    .await;
    assert!(result.is_err(), "unexpected publish on {topic}: {:?}", result.ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn fresh_retained_delivery_is_not_flagged() {
    let mut cfg = node_cfg("stale-fresh", 18881, 17511);
    cfg.retained_staleness_secs = 5;
    let node = entmoot_node::run(cfg).await.unwrap();

    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("stale-fresh-pub", 18881), 16);
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });
    pub_client
        .publish("plant/kiln1/temp", QoS::AtLeastOnce, true, "100")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (sub, mut sub_events) = AsyncClient::new(client_opts("stale-fresh-sub", 18881), 16);
    sub.subscribe("$meta/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut sub_events).await;
    sub.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut sub_events).await;

    let p = await_publish_on(&mut sub_events, "plant/kiln1/temp").await;
    assert_eq!(&p.payload[..], b"100");

    // Well within the 5s bound: no staleness companion should follow.
    assert_no_publish_on(&mut sub_events, "$meta/plant/kiln1/temp", Duration::from_millis(500)).await;

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_retained_delivery_is_flagged_on_meta_topic() {
    let mut cfg = node_cfg("stale-old", 18882, 17512);
    cfg.retained_staleness_secs = 1;
    let node = entmoot_node::run(cfg).await.unwrap();

    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("stale-old-pub", 18882), 16);
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });
    pub_client
        .publish("plant/kiln1/temp", QoS::AtLeastOnce, true, "100")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1300)).await; // exceed the 1s bound

    let (sub, mut sub_events) = AsyncClient::new(client_opts("stale-old-sub", 18882), 16);
    sub.subscribe("$meta/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut sub_events).await;
    sub.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut sub_events).await;

    let data = await_publish_on(&mut sub_events, "plant/kiln1/temp").await;
    assert_eq!(&data.payload[..], b"100");
    let meta = await_publish_on(&mut sub_events, "$meta/plant/kiln1/temp").await;
    let meta_str = String::from_utf8_lossy(&meta.payload);
    assert!(meta_str.contains("stale=true"), "got: {meta_str}");
    assert!(meta_str.contains("bound_secs=1"), "got: {meta_str}");

    node.shutdown().await;
}

/// A namespace-specific override takes precedence over the node-wide default.
#[tokio::test(flavor = "multi_thread")]
async fn per_namespace_staleness_override_wins() {
    let mut cfg = node_cfg("stale-ns", 18883, 17513);
    cfg.retained_staleness_secs = 60; // generous default
    cfg.staleness = vec![StalenessRule { filter: "plant/kiln1/#".into(), bound_secs: 1 }];
    let node = entmoot_node::run(cfg).await.unwrap();

    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("stale-ns-pub", 18883), 16);
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });
    pub_client
        .publish("plant/kiln1/temp", QoS::AtLeastOnce, true, "100")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1300)).await; // past the override, well within the default

    let (sub, mut sub_events) = AsyncClient::new(client_opts("stale-ns-sub", 18883), 16);
    sub.subscribe("$meta/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut sub_events).await;
    sub.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut sub_events).await;

    await_publish_on(&mut sub_events, "plant/kiln1/temp").await;
    // Presence alone (with a timeout) proves the override applied; content
    // is already checked in stale_retained_delivery_is_flagged_on_meta_topic.
    await_publish_on(&mut sub_events, "$meta/plant/kiln1/temp").await;

    node.shutdown().await;
}
