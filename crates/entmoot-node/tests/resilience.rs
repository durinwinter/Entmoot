//! Reconnect-storm protection: connect-admission shedding and retained-match
//! coalescing (workstream 1 of RESILIENCE_ROADMAP.md).
//!
//! `turmoil` was evaluated for these scenarios and parked (see the roadmap):
//! Zenoh owns its own transport/runtime internals that aren't turmoil-aware,
//! so a faithful partition-and-heal simulation isn't achievable without
//! forking Zenoh's transport layer. These tests instead drive a real node
//! with real concurrent rumqttc clients over real sockets, which exercises
//! the same code paths under genuine async concurrency.

use entmoot_core::NodeConfig;
use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions, Packet, QoS};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::time::timeout;

fn node_cfg(id: &str, mqtt_port: u16, zenoh_port: u16) -> NodeConfig {
    NodeConfig {
        id: id.into(),
        mqtt_listen: format!("127.0.0.1:{mqtt_port}"),
        zenoh_listen: vec![format!("tcp/127.0.0.1:{zenoh_port}")],
        peers: vec![],
        scope: "resilience-test".into(),
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

/// A storm of 40 simultaneous CONNECTs against a node admitting at most 5/s
/// (burst 5) must shed the excess with a legible `ServiceUnavailable`
/// CONNACK, not a bare refused socket, and must still admit some clients
/// rather than wedging entirely.
#[tokio::test(flavor = "multi_thread")]
async fn connect_admission_sheds_under_reconnect_storm() {
    let mut cfg = node_cfg("storm-admit", 18871, 17501);
    cfg.connect_admission_rate = 5;
    cfg.connect_admission_burst = 5;
    let node = entmoot_node::run(cfg).await.unwrap();

    const CLIENTS: usize = 40;
    let barrier = Arc::new(Barrier::new(CLIENTS));
    let mut handles = Vec::with_capacity(CLIENTS);
    for i in 0..CLIENTS {
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let (_client, mut events) = AsyncClient::new(client_opts(&format!("storm-{i}"), 18871), 16);
            timeout(Duration::from_secs(5), events.poll()).await.unwrap()
        }));
    }

    let mut admitted = 0usize;
    let mut shed = 0usize;
    for h in handles {
        match h.await.unwrap() {
            Ok(_) => admitted += 1,
            Err(ConnectionError::ConnectionRefused(code)) => {
                assert_eq!(code, rumqttc::ConnectReturnCode::ServiceUnavailable);
                shed += 1;
            }
            other => panic!("unexpected connect outcome: {other:?}"),
        }
    }

    assert_eq!(admitted + shed, CLIENTS);
    assert!(shed > 0, "a storm of {CLIENTS} against burst=5 should shed some connects");
    assert!(admitted > 0, "admission control must still let some clients through");
    assert_eq!(
        node.broker.metrics.connect_shed_total.load(Ordering::Relaxed) as usize,
        shed,
        "connect_shed_total metric should match observed shedding"
    );

    node.shutdown().await;
}

/// A storm of 50 simultaneous SUBSCRIBEs on the same filter (the reconnect-
/// storm shape: many devices resubscribing to e.g. `plant/#`) must coalesce
/// into far fewer than 50 underlying retained-store scans.
#[tokio::test(flavor = "multi_thread")]
async fn retained_match_coalesces_under_subscribe_storm() {
    let node = entmoot_node::run(node_cfg("storm-coalesce", 18872, 17502)).await.unwrap();

    // Seed retained state so there's real scan work to coalesce.
    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("storm-pub", 18872), 16);
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });
    for i in 0..20 {
        pub_client
            .publish(format!("plant/kiln{i}/temp"), QoS::AtLeastOnce, true, "1")
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let scans_before = node.broker.retained.scan_count();

    const CLIENTS: usize = 50;
    let barrier = Arc::new(Barrier::new(CLIENTS));
    let mut handles = Vec::with_capacity(CLIENTS);
    for i in 0..CLIENTS {
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            let (client, mut events) = AsyncClient::new(client_opts(&format!("storm-sub-{i}"), 18872), 16);
            barrier.wait().await;
            client.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
            await_suback(&mut events).await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let scans = node.broker.retained.scan_count() - scans_before;
    assert!(
        scans < (CLIENTS as u64) / 2,
        "expected {CLIENTS} concurrent identical-filter subscribes to coalesce into far fewer than {CLIENTS} scans, got {scans}"
    );

    node.shutdown().await;
}
