//! Phase 1 acceptance tests: retained messages across the mesh (including a
//! node that joins late), authentication, ACL enforcement, and MQTT over TLS.

use entmoot_core::auth::sha256_hex;
use entmoot_core::config::{AclRule, AuthConfig, Policy, TlsConfig, UserCred};
use entmoot_core::NodeConfig;
use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions, Packet, QoS};
use std::time::Duration;
use tokio::time::timeout;

fn node_cfg(id: &str, mqtt_port: u16, zenoh_port: u16, peers: Vec<String>) -> NodeConfig {
    NodeConfig {
        id: id.into(),
        mqtt_listen: format!("127.0.0.1:{mqtt_port}"),
        zenoh_listen: vec![format!("tcp/127.0.0.1:{zenoh_port}")],
        peers,
        scope: "hardening-test".into(),
        ..NodeConfig::default()
    }
}

fn client_opts(name: &str, port: u16) -> MqttOptions {
    let mut opts = MqttOptions::new(name, "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(5));
    opts
}

#[test]
fn bus_listen_config_key_is_accepted() {
    let cfg: NodeConfig = toml::from_str(
        r#"
id = "bus-alias"
mqtt_listen = "127.0.0.1:1883"
bus_listen = ["tcp/127.0.0.1:7447"]
peers = []
"#,
    )
    .unwrap();

    assert_eq!(cfg.zenoh_listen, vec!["tcp/127.0.0.1:7447"]);
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
async fn retained_survives_mesh_and_late_joiners() {
    let node_a = entmoot_node::run(node_cfg("ret-a", 18841, 17481, vec![])).await.unwrap();
    let node_b = entmoot_node::run(node_cfg(
        "ret-b",
        18842,
        17482,
        vec!["tcp/127.0.0.1:17481".into()],
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish retained into node A, with no subscriber anywhere.
    let (pub_client, mut pub_events) = AsyncClient::new(client_opts("ret-pub", 18841), 16);
    tokio::spawn(async move { while pub_events.poll().await.is_ok() {} });
    pub_client
        .publish("plant/kiln1/status", QoS::AtLeastOnce, true, "BIRTH")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // A brand-new subscriber on node B must get it, flagged retained.
    let (sub_client, mut sub_events) = AsyncClient::new(client_opts("ret-sub", 18842), 16);
    sub_client.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    let p = await_publish(&mut sub_events).await;
    assert_eq!(p.topic, "plant/kiln1/status");
    assert_eq!(&p.payload[..], b"BIRTH");
    assert!(p.retain, "delivery on subscribe must carry the retain flag");

    // A node that joins AFTER the publish catches up via the queryable.
    let node_c = entmoot_node::run(node_cfg(
        "ret-c",
        18843,
        17483,
        vec!["tcp/127.0.0.1:17481".into()],
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await; // > FETCH_DELAY
    let (late_client, mut late_events) = AsyncClient::new(client_opts("ret-late", 18843), 16);
    late_client.subscribe("plant/kiln1/+", QoS::AtLeastOnce).await.unwrap();
    let p = await_publish(&mut late_events).await;
    assert_eq!(&p.payload[..], b"BIRTH");
    assert!(p.retain);

    // An empty retained payload clears the slot mesh-wide.
    let (clr_client, mut clr_events) = AsyncClient::new(client_opts("ret-clr", 18842), 16);
    tokio::spawn(async move { while clr_events.poll().await.is_ok() {} });
    clr_client
        .publish("plant/kiln1/status", QoS::AtLeastOnce, true, "")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(node_a.broker.retained.matching("#").is_empty(), "clear must propagate");

    node_a.shutdown().await;
    node_b.shutdown().await;
    node_c.shutdown().await;
}

fn secured_cfg(mqtt_port: u16, zenoh_port: u16) -> NodeConfig {
    let mut cfg = node_cfg("sec", mqtt_port, zenoh_port, vec![]);
    cfg.auth = AuthConfig {
        allow_anonymous: false,
        default_policy: Policy::Deny,
        users: vec![UserCred { name: "ops".into(), password_sha256: sha256_hex("hunter2") }],
        jwt: None,
    };
    cfg.acl = vec![AclRule {
        user: "ops".into(),
        publish: vec!["plant/#".into()],
        subscribe: vec!["plant/#".into()],
    }];
    cfg
}

#[tokio::test(flavor = "multi_thread")]
async fn auth_is_enforced() {
    let node = entmoot_node::run(secured_cfg(18844, 17484)).await.unwrap();

    // Anonymous: refused.
    let (_c, mut events) = AsyncClient::new(client_opts("anon", 18844), 16);
    match timeout(Duration::from_secs(5), events.poll()).await.unwrap() {
        Err(ConnectionError::ConnectionRefused(code)) => {
            assert_eq!(code, rumqttc::ConnectReturnCode::NotAuthorized)
        }
        other => panic!("anonymous connect should be refused, got {other:?}"),
    }

    // Wrong password: refused.
    let mut opts = client_opts("badpw", 18844);
    opts.set_credentials("ops", "wrong");
    let (_c, mut events) = AsyncClient::new(opts, 16);
    match timeout(Duration::from_secs(5), events.poll()).await.unwrap() {
        Err(ConnectionError::ConnectionRefused(code)) => {
            assert_eq!(code, rumqttc::ConnectReturnCode::BadUserNamePassword)
        }
        other => panic!("bad password should be refused, got {other:?}"),
    }

    // Correct credentials: full round trip.
    let mut opts = client_opts("good", 18844);
    opts.set_credentials("ops", "hunter2");
    let (client, mut events) = AsyncClient::new(opts, 16);
    client.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut events).await;
    client
        .publish("plant/a", QoS::AtLeastOnce, false, "ok")
        .await
        .unwrap();
    let p = await_publish(&mut events).await;
    assert_eq!(&p.payload[..], b"ok");

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn acl_blocks_unauthorized_topics() {
    let node = entmoot_node::run(secured_cfg(18845, 17485)).await.unwrap();

    let mut opts = client_opts("ops-acl", 18845);
    opts.set_credentials("ops", "hunter2");
    let (client, mut events) = AsyncClient::new(opts, 16);

    // Subscribing outside the grant fails; inside succeeds.
    client.subscribe("cmd/#", QoS::AtLeastOnce).await.unwrap();
    let ack = await_suback(&mut events).await;
    assert_eq!(ack.return_codes, vec![rumqttc::SubscribeReasonCode::Failure]);
    client.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    let ack = await_suback(&mut events).await;
    assert_eq!(
        ack.return_codes,
        vec![rumqttc::SubscribeReasonCode::Success(QoS::AtLeastOnce)]
    );

    // A publish outside the grant is dropped (but acked); inside arrives.
    client.publish("cmd/plc9/reboot", QoS::AtLeastOnce, false, "nope").await.unwrap();
    client.publish("plant/ok", QoS::AtLeastOnce, false, "yes").await.unwrap();
    let p = await_publish(&mut events).await;
    assert_eq!(p.topic, "plant/ok", "denied publish must not be routed");

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_over_tls_works() {
    // rumqttc's rustls resolves the process-default provider; the node itself
    // pins its provider explicitly and doesn't need this.
    tokio_rustls::rustls::crypto::ring::default_provider().install_default().ok();

    // Self-signed cert for localhost, written to disk like an operator would.
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = std::env::temp_dir().join(format!("entmoot-tls-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cert_path = dir.join("server.pem");
    let key_path = dir.join("server.key");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();

    let mut cfg = node_cfg("tls", 18846, 17486, vec![]);
    cfg.tls = Some(TlsConfig {
        listen: "127.0.0.1:18856".into(),
        cert_file: cert_path.to_string_lossy().into_owned(),
        key_file: key_path.to_string_lossy().into_owned(),
        client_ca_file: None,
    });
    let node = entmoot_node::run(cfg).await.unwrap();

    let mut opts = MqttOptions::new("tls-client", "localhost", 18856);
    opts.set_keep_alive(Duration::from_secs(5));
    opts.set_transport(rumqttc::Transport::Tls(rumqttc::TlsConfiguration::Simple {
        ca: cert.cert.pem().into_bytes(),
        alpn: None,
        client_auth: None,
    }));
    let (client, mut events) = AsyncClient::new(opts, 16);
    client.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut events).await;
    client
        .publish("plant/secure", QoS::AtLeastOnce, false, "over-tls")
        .await
        .unwrap();
    let p = await_publish(&mut events).await;
    assert_eq!(&p.payload[..], b"over-tls");

    node.shutdown().await;
    std::fs::remove_dir_all(&dir).ok();
}
