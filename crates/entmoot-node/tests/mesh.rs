//! The Phase 0 acceptance test: a publish entering node B reaches a
//! subscriber connected to node A, purely via the zenoh peer link.

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
        scope: "entmoot-test".into(),
        ..NodeConfig::default()
    }
}

async fn mqtt_client(name: &str, port: u16) -> (AsyncClient, rumqttc::EventLoop) {
    let mut opts = MqttOptions::new(name, "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(5));
    AsyncClient::new(opts, 32)
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_crosses_the_mesh() {
    let node_a = entmoot_node::run(node_cfg("test-a", 18831, 17471, vec![])).await.unwrap();
    let node_b = entmoot_node::run(node_cfg(
        "test-b",
        18832,
        17472,
        vec!["tcp/127.0.0.1:17471".into()],
    ))
    .await
    .unwrap();

    // Give the zenoh peers a moment to establish the link.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (sub_client, mut sub_events) = mqtt_client("it-sub", 18831).await;
    sub_client.subscribe("plant/+/temp", QoS::AtLeastOnce).await.unwrap();

    // Drive the subscriber loop until SUBACK so the zenoh subscriber exists
    // (and has propagated) before we publish.
    timeout(Duration::from_secs(5), async {
        loop {
            if let Event::Incoming(Packet::SubAck(_)) = sub_events.poll().await.unwrap() {
                break;
            }
        }
    })
    .await
    .expect("no SUBACK");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (pub_client, mut pub_events) = mqtt_client("it-pub", 18832).await;
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });
    pub_client
        .publish("plant/kiln1/temp", QoS::AtLeastOnce, false, "993.5")
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(5), async {
        loop {
            if let Event::Incoming(Packet::Publish(p)) = sub_events.poll().await.unwrap() {
                return p;
            }
        }
    })
    .await
    .expect("publish did not cross the mesh");

    assert_eq!(received.topic, "plant/kiln1/temp");
    assert_eq!(&received.payload[..], b"993.5");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_subscription_gets_suback_failure() {
    let node = entmoot_node::run(node_cfg("test-c", 18833, 17473, vec![])).await.unwrap();

    let (client, mut events) = mqtt_client("it-badsub", 18833).await;
    // 'a//b' has an empty level: must be rejected, not silently accepted.
    client.subscribe("a//b", QoS::AtLeastOnce).await.unwrap();

    let ack = timeout(Duration::from_secs(5), async {
        loop {
            if let Event::Incoming(Packet::SubAck(a)) = events.poll().await.unwrap() {
                return a;
            }
        }
    })
    .await
    .expect("no SUBACK");
    assert_eq!(ack.return_codes, vec![rumqttc::SubscribeReasonCode::Failure]);

    node.shutdown().await;
}
