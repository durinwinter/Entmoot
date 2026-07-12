//! Data validation (Entmoot's slice of HiveMQ's Data Governance Hub schema
//! policies, see ENTERPRISE_ROADMAP.md): publishes on a matching topic must
//! conform to a configured JSON Schema, or the rule's `on_fail` action
//! applies — drop (acked, not delivered) or disconnect the client.

use entmoot_core::config::{SchemaFailAction, SchemaRule};
use entmoot_core::NodeConfig;
use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions, Packet, QoS};
use std::time::Duration;
use tokio::time::timeout;

fn node_cfg(id: &str, mqtt_port: u16, zenoh_port: u16, rules: Vec<SchemaRule>) -> NodeConfig {
    NodeConfig {
        id: id.into(),
        mqtt_listen: format!("127.0.0.1:{mqtt_port}"),
        zenoh_listen: vec![format!("tcp/127.0.0.1:{zenoh_port}")],
        peers: vec![],
        scope: "schema-test".into(),
        schema: rules,
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

fn temp_schema_rule(action: SchemaFailAction) -> SchemaRule {
    SchemaRule {
        filter: "plant/+/temp".into(),
        schema: r#"{"type":"object","properties":{"value":{"type":"number"}},"required":["value"]}"#.into(),
        on_fail: action,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conforming_publish_is_delivered() {
    let node = entmoot_node::run(node_cfg("schema-ok", 18911, 17541, vec![temp_schema_rule(SchemaFailAction::Drop)]))
        .await
        .unwrap();

    let (sub, mut sub_events) = AsyncClient::new(client_opts("schema-ok-sub", 18911), 16);
    sub.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut sub_events).await;

    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("schema-ok-pub", 18911), 16);
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });
    pub_client
        .publish("plant/kiln1/temp", QoS::AtLeastOnce, false, r#"{"value": 93.5}"#)
        .await
        .unwrap();

    let p = await_publish(&mut sub_events).await;
    assert_eq!(&p.payload[..], br#"{"value": 93.5}"#);

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nonconforming_publish_is_dropped_not_delivered() {
    let node = entmoot_node::run(node_cfg(
        "schema-drop",
        18912,
        17542,
        vec![temp_schema_rule(SchemaFailAction::Drop)],
    ))
    .await
    .unwrap();

    let (sub, mut sub_events) = AsyncClient::new(client_opts("schema-drop-sub", 18912), 16);
    sub.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut sub_events).await;

    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("schema-drop-pub", 18912), 16);
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });
    // Fails the schema: "value" must be a number.
    pub_client
        .publish("plant/kiln1/temp", QoS::AtLeastOnce, false, r#"{"value": "hot"}"#)
        .await
        .unwrap();
    // A conforming publish afterwards proves the connection is still alive
    // and the mesh is otherwise working — only the bad one was dropped.
    pub_client
        .publish("plant/kiln1/temp", QoS::AtLeastOnce, false, r#"{"value": 12}"#)
        .await
        .unwrap();

    let p = await_publish(&mut sub_events).await;
    assert_eq!(&p.payload[..], br#"{"value": 12}"#, "the invalid publish must not have been delivered");

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nonconforming_publish_disconnects_when_configured() {
    let node = entmoot_node::run(node_cfg(
        "schema-kick",
        18913,
        17543,
        vec![temp_schema_rule(SchemaFailAction::Disconnect)],
    ))
    .await
    .unwrap();

    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("schema-kick-pub", 18913), 16);
    pub_client
        .publish("plant/kiln1/temp", QoS::AtLeastOnce, false, r#"{"value": "hot"}"#)
        .await
        .unwrap();

    let result = timeout(Duration::from_secs(5), async {
        loop {
            match pub_events.poll().await {
                Err(ConnectionError::Io(_)) | Err(ConnectionError::MqttState(_)) => return,
                Err(_) => return,
                Ok(_) => {}
            }
        }
    })
    .await;
    assert!(result.is_ok(), "expected the connection to be closed after a disconnect-on-fail schema violation");

    node.shutdown().await;
}
