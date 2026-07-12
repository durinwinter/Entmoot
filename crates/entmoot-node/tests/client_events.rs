//! Visualizer honesty (workstream 6 of RESILIENCE_ROADMAP.md): client
//! connect/subscribe/unsubscribe/disconnect events are emitted onto
//! `$meta/clients/<node-id>/<client-id>`, the same mesh-wide pub/sub path as
//! `$SYS`, so a dashboard can key liveness off actual MQTT session activity
//! (carried over Zenoh's own session keepalives) instead of guessing from
//! tunnel/link state.

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
        scope: "client-events-test".into(),
        ..NodeConfig::default()
    }
}

fn client_opts(name: &str, port: u16) -> MqttOptions {
    let mut opts = MqttOptions::new(name, "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(5));
    opts
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

/// Polls until a PUBLISH whose topic starts with `prefix` and payload
/// contains `needle` arrives, discarding anything else.
async fn await_event(events: &mut rumqttc::EventLoop, prefix: &str, needle: &str) -> rumqttc::Publish {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Event::Incoming(Packet::Publish(p)) = events.poll().await.unwrap() {
                if p.topic.starts_with(prefix) && String::from_utf8_lossy(&p.payload).contains(needle) {
                    return p;
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no event matching prefix {prefix:?} containing {needle:?}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn connect_subscribe_unsubscribe_disconnect_are_all_visible_on_meta_clients() {
    let node = entmoot_node::run(node_cfg("events-node", 18901, 17531)).await.unwrap();

    let (watcher, mut watcher_events) = AsyncClient::new(client_opts("events-watcher", 18901), 16);
    watcher.subscribe("$meta/clients/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut watcher_events).await;

    let prefix = format!("$meta/clients/{}/events-target", "events-node");

    let mut opts = client_opts("events-target", 18901);
    opts.set_clean_session(false);
    let (target, mut target_events) = AsyncClient::new(opts, 16);
    tokio::spawn(async move {
        loop {
            if target_events.poll().await.is_err() {
                return;
            }
        }
    });

    let connect_evt = await_event(&mut watcher_events, &prefix, "connect").await;
    assert!(String::from_utf8_lossy(&connect_evt.payload).contains("clean=false"));

    target.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    let sub_evt = await_event(&mut watcher_events, &prefix, "subscribe").await;
    assert!(String::from_utf8_lossy(&sub_evt.payload).contains("filter=plant/#"));

    target.unsubscribe("plant/#").await.unwrap();
    let unsub_evt = await_event(&mut watcher_events, &prefix, "unsubscribe").await;
    assert!(String::from_utf8_lossy(&unsub_evt.payload).contains("filter=plant/#"));

    target.disconnect().await.unwrap();
    let disconnect_evt = await_event(&mut watcher_events, &prefix, "disconnect").await;
    assert!(String::from_utf8_lossy(&disconnect_evt.payload).contains("reason=clean"));

    node.shutdown().await;
}
