//! Transport hygiene (workstream 5 of RESILIENCE_ROADMAP.md): clamping
//! Zenoh's wire batch size (its MTU equivalent) below a link's real path
//! MTU. This test only proves the config knob wires through and the mesh
//! still works with it set to a realistically small value (1200, comfortably
//! under a 1280-byte Nebula/WireGuard-safe MTU) — actually discovering a
//! real path MTU needs `scripts/mtu-sweep.sh` against a real link.

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
        scope: "transport-test".into(),
        zenoh_link_mtu: Some(1200),
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

#[tokio::test(flavor = "multi_thread")]
async fn mesh_works_with_clamped_link_mtu() {
    let node_a = entmoot_node::run(node_cfg("mtu-a", 18891, 17521, vec![])).await.unwrap();
    let node_b = entmoot_node::run(node_cfg(
        "mtu-b",
        18892,
        17522,
        vec!["tcp/127.0.0.1:17521".into()],
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (sub_client, mut sub_events) = AsyncClient::new(client_opts("mtu-sub", 18892), 16);
    sub_client.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut sub_events).await;
    tokio::time::sleep(Duration::from_millis(500)).await; // let the zenoh subscriber propagate

    // A payload comfortably larger than the clamped 1200-byte link batch
    // size, to exercise Zenoh's own fragmentation across multiple batches
    // rather than a single undersized one.
    let big_payload = vec![7u8; 4096];
    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("mtu-pub", 18891), 16);
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });
    pub_client
        .publish("plant/kiln1/blob", QoS::AtLeastOnce, false, big_payload.clone())
        .await
        .unwrap();

    let p = await_publish(&mut sub_events).await;
    assert_eq!(p.topic, "plant/kiln1/blob");
    assert_eq!(p.payload.len(), big_payload.len());
    assert_eq!(&p.payload[..], &big_payload[..]);

    node_a.shutdown().await;
    node_b.shutdown().await;
}
