//! Phase 1b acceptance tests: persistent sessions with offline QoS 1
//! queueing, retained persistence across a node restart, mTLS client-cert
//! identity, the Prometheus metrics endpoint, slow-consumer eviction, and
//! $SYS node-stats topics.

use entmoot_core::config::{AclRule, AuthConfig, Policy, TlsConfig};
use entmoot_core::NodeConfig;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

fn node_cfg(id: &str, mqtt_port: u16, zenoh_port: u16) -> NodeConfig {
    NodeConfig {
        id: id.into(),
        mqtt_listen: format!("127.0.0.1:{mqtt_port}"),
        zenoh_listen: vec![format!("tcp/127.0.0.1:{zenoh_port}")],
        scope: "p1b-test".into(),
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

#[tokio::test(flavor = "multi_thread")]
async fn persistent_session_queues_while_offline() {
    let node = entmoot_node::run(node_cfg("ps", 18861, 17491)).await.unwrap();

    // Device connects with cleanSession=0 and subscribes.
    let mut opts = client_opts("plc-7", 18861);
    opts.set_clean_session(false);
    let (client, mut events) = AsyncClient::new(opts, 16);
    client.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut events).await;

    // Network dies (drop = TCP close, no DISCONNECT packet).
    drop(client);
    drop(events);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Traffic keeps flowing while the device is away.
    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("feeder", 18861), 16);
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });
    for i in 1..=3 {
        pub_client
            .publish("plant/kiln1/temp", QoS::AtLeastOnce, false, format!("t-{i}"))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Device reconnects: session resumes, backlog arrives without resubscribing.
    let mut opts = client_opts("plc-7", 18861);
    opts.set_clean_session(false);
    let (_client, mut events) = AsyncClient::new(opts, 16);
    let connack = timeout(Duration::from_secs(5), async {
        loop {
            if let Event::Incoming(Packet::ConnAck(c)) = events.poll().await.unwrap() {
                return c;
            }
        }
    })
    .await
    .expect("no CONNACK");
    assert!(connack.session_present, "resumed session must set session_present");

    for i in 1..=3 {
        let p = await_publish(&mut events).await;
        assert_eq!(p.topic, "plant/kiln1/temp");
        assert_eq!(p.payload, format!("t-{i}"), "backlog must arrive in order");
        assert_eq!(p.qos, QoS::AtLeastOnce);
    }

    // And the live path works again after the drain.
    pub_client
        .publish("plant/kiln1/temp", QoS::AtLeastOnce, false, "live")
        .await
        .unwrap();
    let p = await_publish(&mut events).await;
    assert_eq!(&p.payload[..], b"live");

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn retained_survives_node_restart() {
    let dir = std::env::temp_dir().join(format!("entmoot-p1b-data-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let mut cfg = node_cfg("rp-1", 18862, 17492);
    cfg.data_dir = Some(dir.to_string_lossy().into_owned());
    let node = entmoot_node::run(cfg).await.unwrap();

    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("rp-pub", 18862), 16);
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });
    pub_client
        .publish("plant/kiln1/config", QoS::AtLeastOnce, true, "setpoint=993")
        .await
        .unwrap();

    // Wait past the debounced snapshot interval, then kill the whole node.
    tokio::time::sleep(Duration::from_millis(3000)).await;
    node.shutdown().await;

    // Fresh process, same data_dir (new ports: nothing lingers from run 1).
    let mut cfg = node_cfg("rp-2", 18863, 17493);
    cfg.data_dir = Some(dir.to_string_lossy().into_owned());
    let node = entmoot_node::run(cfg).await.unwrap();

    let (sub_client, mut sub_events) = AsyncClient::new(client_opts("rp-sub", 18863), 16);
    sub_client.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    let p = await_publish(&mut sub_events).await;
    assert_eq!(p.topic, "plant/kiln1/config");
    assert_eq!(&p.payload[..], b"setpoint=993");
    assert!(p.retain);

    node.shutdown().await;
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread")]
async fn mtls_cn_is_the_acl_identity() {
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};

    tokio_rustls::rustls::crypto::ring::default_provider().install_default().ok();

    // A tiny PKI: CA -> server cert (localhost) + client cert (CN=plc1).
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.distinguished_name.push(DnType::CommonName, "entmoot-test-ca");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, &ca_key);

    let server_key = KeyPair::generate().unwrap();
    let server_cert = CertificateParams::new(vec!["localhost".into()])
        .unwrap()
        .signed_by(&server_key, &issuer)
        .unwrap();

    let client_key = KeyPair::generate().unwrap();
    let mut client_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    client_params.distinguished_name.push(DnType::CommonName, "plc1");
    let client_cert = client_params.signed_by(&client_key, &issuer).unwrap();

    let dir = std::env::temp_dir().join(format!("entmoot-p1b-mtls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let ca_path = dir.join("ca.pem");
    let cert_path = dir.join("server.pem");
    let key_path = dir.join("server.key");
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();
    std::fs::write(&cert_path, server_cert.pem()).unwrap();
    std::fs::write(&key_path, server_key.serialize_pem()).unwrap();

    // No password users at all: identity comes from the certificate.
    let mut cfg = node_cfg("mtls", 18864, 17494);
    cfg.auth = AuthConfig {
        allow_anonymous: false,
        default_policy: Policy::Deny,
        users: vec![],
    };
    cfg.acl = vec![AclRule {
        user: "plc1".into(),
        publish: vec!["plant/#".into()],
        subscribe: vec!["plant/#".into()],
    }];
    cfg.tls = Some(TlsConfig {
        listen: "127.0.0.1:18874".into(),
        cert_file: cert_path.to_string_lossy().into_owned(),
        key_file: key_path.to_string_lossy().into_owned(),
        client_ca_file: Some(ca_path.to_string_lossy().into_owned()),
    });
    let node = entmoot_node::run(cfg).await.unwrap();

    let mut opts = MqttOptions::new("plc1-dev", "localhost", 18874);
    opts.set_keep_alive(Duration::from_secs(5));
    opts.set_transport(rumqttc::Transport::Tls(rumqttc::TlsConfiguration::Simple {
        ca: ca_cert.pem().into_bytes(),
        alpn: None,
        client_auth: Some((client_cert.pem().into_bytes(), client_key.serialize_pem().into_bytes())),
    }));
    let (client, mut events) = AsyncClient::new(opts, 16);

    // ACL grant for CN "plc1" applies; outside the grant is refused.
    client.subscribe("cmd/#", QoS::AtLeastOnce).await.unwrap();
    let ack = await_suback(&mut events).await;
    assert_eq!(ack.return_codes, vec![rumqttc::SubscribeReasonCode::Failure]);

    client.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    let ack = await_suback(&mut events).await;
    assert_eq!(
        ack.return_codes,
        vec![rumqttc::SubscribeReasonCode::Success(QoS::AtLeastOnce)]
    );
    client
        .publish("plant/mtls", QoS::AtLeastOnce, false, "cert-auth")
        .await
        .unwrap();
    let p = await_publish(&mut events).await;
    assert_eq!(&p.payload[..], b"cert-auth");

    node.shutdown().await;
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_consumer_is_evicted() {
    let mut cfg = node_cfg("slow", 18866, 17496);
    cfg.slow_consumer_grace_ms = 200;
    let node = entmoot_node::run(cfg).await.unwrap();

    // The victim subscribes, then stops polling its event loop: its socket is
    // never read again, so the node's outbound queue eventually jams.
    let mut opts = client_opts("stalled-scada", 18866);
    opts.set_keep_alive(Duration::from_secs(120)); // don't let keep-alive win the race
    let (victim, mut victim_events) = AsyncClient::new(opts, 16);
    victim.subscribe("plant/#", QoS::AtMostOnce).await.unwrap();
    await_suback(&mut victim_events).await;

    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("firehose", 18866), 64);
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });

    let evictions = || {
        node.broker
            .metrics
            .slow_consumer_evictions_total
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    // Stay under rumqttc's 10 KiB default packet-size cap.
    let payload = vec![0u8; 8 * 1024];
    let mut fired = false;
    for _ in 0..4000 {
        pub_client
            .publish("plant/kiln1/blob", QoS::AtMostOnce, false, payload.clone())
            .await
            .unwrap();
        if evictions() > 0 {
            fired = true;
            break;
        }
    }
    // The queue jams mid-run; give the grace period a moment to expire.
    for _ in 0..50 {
        if evictions() > 0 {
            fired = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(fired, "slow consumer was never evicted");

    // The victim's connection is actually gone (only the publisher remains).
    timeout(Duration::from_secs(5), async {
        while node.broker.connections() > 1 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("evicted connection was not closed");

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sys_topics_report_node_stats() {
    let mut cfg = node_cfg("sysx", 18867, 17497);
    cfg.sys_interval_secs = 1;
    let node = entmoot_node::run(cfg).await.unwrap();

    let (client, mut events) = AsyncClient::new(client_opts("ops", 18867), 16);
    client.subscribe("$SYS/#", QoS::AtMostOnce).await.unwrap();
    let ack = await_suback(&mut events).await;
    assert_eq!(ack.return_codes, vec![rumqttc::SubscribeReasonCode::Success(QoS::AtMostOnce)]);

    // Stats arrive under this node's id; values are readable numbers/strings.
    let p = await_publish(&mut events).await;
    assert!(
        p.topic.starts_with("$SYS/broker/sysx/"),
        "unexpected $SYS topic {:?}",
        p.topic
    );
    std::str::from_utf8(&p.payload).expect("$SYS payloads are UTF-8");

    // MQTT-4.7.2-1: a plain '#' subscription must NOT see $SYS traffic.
    let (snoop, mut snoop_events) = AsyncClient::new(client_opts("snoop", 18867), 16);
    snoop.subscribe("#", QoS::AtMostOnce).await.unwrap();
    await_suback(&mut snoop_events).await;
    let leaked = timeout(Duration::from_millis(2500), async {
        loop {
            if let Event::Incoming(Packet::Publish(p)) = snoop_events.poll().await.unwrap() {
                return p;
            }
        }
    })
    .await;
    assert!(leaked.is_err(), "'#' subscription leaked $SYS traffic: {leaked:?}");

    // Clients cannot publish into $SYS (rejected as an invalid topic).
    // The node treats it as a protocol violation and drops the connection.
    client
        .publish("$SYS/broker/sysx/version", QoS::AtMostOnce, false, "forged")
        .await
        .unwrap();
    let disconnected = timeout(Duration::from_secs(5), async {
        loop {
            if events.poll().await.is_err() {
                return;
            }
        }
    })
    .await;
    assert!(disconnected.is_ok(), "$SYS publish was not rejected");

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_endpoint_scrapes() {
    let mut cfg = node_cfg("mx", 18865, 17495);
    cfg.metrics_listen = Some("127.0.0.1:19464".into());
    let node = entmoot_node::run(cfg).await.unwrap();

    // Generate one in + one out message.
    let (client, mut events) = AsyncClient::new(client_opts("mx-client", 18865), 16);
    client.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut events).await;
    client.publish("plant/m", QoS::AtLeastOnce, false, "x").await.unwrap();
    await_publish(&mut events).await;

    let mut stream = tokio::net::TcpStream::connect("127.0.0.1:19464").await.unwrap();
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut body = String::new();
    stream.read_to_string(&mut body).await.unwrap();

    assert!(body.starts_with("HTTP/1.1 200 OK"), "got: {body}");
    for needle in [
        r#"entmoot_connections_current{node="mx"} 1"#,
        r#"entmoot_messages_in_total{node="mx"} 1"#,
        r#"entmoot_messages_out_total{node="mx"} 1"#,
        r#"entmoot_sessions{node="mx"} 1"#,
    ] {
        assert!(body.contains(needle), "missing {needle:?} in:\n{body}");
    }

    node.shutdown().await;
}
