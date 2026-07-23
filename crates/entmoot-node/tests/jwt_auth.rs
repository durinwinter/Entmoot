//! JWT bearer-token auth (Entmoot's take on HiveMQ's Enterprise Security
//! Extension OAuth2/JWT support — HS256, static shared secret, no JWKS/OIDC
//! discovery — see ENTERPRISE_ROADMAP.md). Additive to local password auth:
//! a CONNECT whose username isn't a known local user gets its password
//! tried as a JWT instead of an outright refusal.

use entmoot_core::config::{AclRule, AuthConfig, Policy, UserCred};
use entmoot_core::auth::sha256_hex;
use entmoot_core::config::JwtConfig;
use entmoot_core::NodeConfig;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions, Packet, QoS};
use serde_json::json;
use std::time::Duration;
use tokio::time::timeout;

const SECRET: &str = "test-hmac-secret";

fn sign(claims: serde_json::Value) -> String {
    encode(&Header::new(Algorithm::HS256), &claims, &EncodingKey::from_secret(SECRET.as_bytes())).unwrap()
}

fn node_cfg(id: &str, mqtt_port: u16, zenoh_port: u16) -> NodeConfig {
    let mut cfg = NodeConfig {
        id: id.into(),
        mqtt_listen: format!("127.0.0.1:{mqtt_port}"),
        zenoh_listen: vec![format!("tcp/127.0.0.1:{zenoh_port}")],
        peers: vec![],
        scope: "jwt-test".into(),
        ..NodeConfig::default()
    };
    cfg.auth = AuthConfig {
        allow_anonymous: false,
        default_policy: Policy::Deny,
        users: vec![UserCred { name: "legacy-plc".into(), password_sha256: sha256_hex("hunter2") }],
        jwt: Some(JwtConfig {
            hmac_secret: SECRET.into(),
            identity_claim: "sub".into(),
            issuer: Some("entmoot-test-idp".into()),
            audience: None,
        }),
    };
    cfg.acl = vec![
        AclRule { user: "legacy-plc".into(), publish: vec!["plant/#".into()], subscribe: vec![] },
        AclRule { user: "gateway-7".into(), publish: vec!["plant/#".into()], subscribe: vec!["plant/#".into()] },
    ];
    cfg
}

fn client_opts(name: &str, port: u16, user: &str, password: &str) -> MqttOptions {
    let mut opts = MqttOptions::new(name, "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(5));
    opts.set_credentials(user, password);
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

#[tokio::test(flavor = "multi_thread")]
async fn valid_jwt_authenticates_and_acl_applies_to_its_identity() {
    let node = entmoot_node::run(node_cfg("jwt-ok", 18931, 17561)).await.unwrap();

    let token = sign(json!({"sub": "gateway-7", "iss": "entmoot-test-idp", "exp": 9_999_999_999u64}));
    let (client, mut events) = AsyncClient::new(client_opts("gw7", 18931, "ignored-username", &token), 16);
    client.subscribe("plant/#", QoS::AtLeastOnce).await.unwrap();
    await_suback(&mut events).await;
    client.publish("plant/kiln1/temp", QoS::AtLeastOnce, false, "hot").await.unwrap();
    let p = await_publish(&mut events).await;
    assert_eq!(&p.payload[..], b"hot");

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_jwt_is_refused() {
    let node = entmoot_node::run(node_cfg("jwt-bad", 18932, 17562)).await.unwrap();

    let expired = sign(json!({"sub": "gateway-7", "iss": "entmoot-test-idp", "exp": 1u64}));
    let (_c, mut events) = AsyncClient::new(client_opts("gw7-expired", 18932, "x", &expired), 16);
    match timeout(Duration::from_secs(5), events.poll()).await.unwrap() {
        Err(ConnectionError::ConnectionRefused(code)) => {
            assert_eq!(code, rumqttc::ConnectReturnCode::BadUserNamePassword)
        }
        other => panic!("expired JWT should be refused, got {other:?}"),
    }

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn known_local_user_still_requires_its_own_password_not_a_token() {
    let node = entmoot_node::run(node_cfg("jwt-legacy", 18933, 17563)).await.unwrap();

    // A JWT presented under the *known local username* must not work —
    // that username must authenticate with its own password.
    let token = sign(json!({"sub": "gateway-7", "iss": "entmoot-test-idp", "exp": 9_999_999_999u64}));
    let (_c, mut events) = AsyncClient::new(client_opts("legacy-confused", 18933, "legacy-plc", &token), 16);
    match timeout(Duration::from_secs(5), events.poll()).await.unwrap() {
        Err(ConnectionError::ConnectionRefused(code)) => {
            assert_eq!(code, rumqttc::ConnectReturnCode::BadUserNamePassword)
        }
        other => panic!("token under a known username must not authenticate, got {other:?}"),
    }

    // The legacy user's real password still works exactly as before.
    let (client, mut good_events) = AsyncClient::new(client_opts("legacy-real", 18933, "legacy-plc", "hunter2"), 16);
    client.publish("plant/kiln1/temp", QoS::AtLeastOnce, false, "ok").await.unwrap();
    let ack = timeout(Duration::from_secs(5), good_events.poll()).await.unwrap();
    assert!(ack.is_ok(), "legacy password auth must still work: {ack:?}");

    node.shutdown().await;
}
