//! Per-client MQTT 3.1.1 connection handling.
//!
//! Inbound PUBLISH -> zenoh `put` (plus a put/delete on the retained keyspace
//! when the retain flag is set). SUBSCRIBE -> zenoh subscriber owned by the
//! client's *session*, so cleanSession=0 subscriptions keep collecting after
//! the connection dies (see `session.rs`). QoS 2 inbound is accepted with the
//! full PUBREC/PUBREL/PUBCOMP handshake but relayed with at-least-once
//! semantics across the mesh.
//!
//! Hardening enforced here: connect-admission control (CONNECTs beyond the
//! configured rate are refused with `ServiceUnavailable` before any auth or
//! session work, ahead of the reconnect-storm path in `admission.rs`),
//! reconnect-churn quarantine (a specific client id reconnecting too often
//! is refused for a cooldown, see `churn.rs`), password or client-cert auth
//! on CONNECT, per-identity topic ACLs (denied publishes are dropped and
//! logged, mosquitto-style, to avoid reconnect storms from misconfigured
//! devices; denied subscriptions get a SUBACK failure), a per-connection
//! publish rate limit whose violators are disconnected, and JSON Schema
//! data validation (`entmoot_core::schema`) on publishes matching a
//! configured topic filter — dropped or disconnected per the rule's
//! `on_fail` action. Auth failures and ACL denials (publish/subscribe/will)
//! are both `tracing::warn!`-logged locally and published onto
//! `$meta/clients` for a mesh-wide audit stream (see `publish_client_event`);
//! a live connection can also be force-disconnected mesh-wide via
//! `ctl.rs`'s control-center-lite query.

use crate::metrics::Metrics;
use crate::session::{activate_subscription, SessionState};
use crate::Broker;
use anyhow::{anyhow, bail, Context, Result};
use bytes::BytesMut;
use entmoot_core::auth::ConnectDenied;
use entmoot_core::config::SchemaFailAction;
use entmoot_core::schema::Verdict as SchemaVerdict;
use entmoot_core::topic;
use mqttbytes::v4::{
    self, ConnAck, ConnectReturnCode, LastWill, Packet, PingResp, PubAck, PubComp, PubRec,
    Publish, SubAck, SubscribeReasonCode, UnsubAck,
};
use mqttbytes::QoS;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const OUTBOUND_QUEUE: usize = 1024;

/// `cert_identity` is set when the client authenticated with an mTLS client
/// certificate; it overrides any MQTT username/password.
pub async fn serve<S>(
    broker: Arc<Broker>,
    stream: S,
    peer: SocketAddr,
    cert_identity: Option<String>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut conn = MqttConn::new(stream, broker.cfg.max_packet_size);

    // MQTT-3.1.0-1: first packet MUST be CONNECT, and it must arrive promptly.
    let connect = match tokio::time::timeout(CONNECT_TIMEOUT, conn.read_packet()).await {
        Ok(Ok(Packet::Connect(c))) => c,
        Ok(Ok(other)) => bail!("first packet was {other:?}, expected CONNECT"),
        Ok(Err(e)) => return Err(e).context("reading CONNECT"),
        Err(_) => bail!("client sent no CONNECT within {CONNECT_TIMEOUT:?}"),
    };

    if !broker.connect_admission.admit() {
        warn!(addr = %peer, "CONNECT shed: connect-admission rate exceeded");
        Metrics::bump(&broker.metrics.connect_shed_total);
        conn.write_packet(&Packet::ConnAck(ConnAck::new(ConnectReturnCode::ServiceUnavailable, false)))
            .await?;
        return Ok(());
    }

    let identity = match cert_identity {
        Some(cn) => {
            debug!(addr = %peer, cn = %cn, "identity from client certificate");
            cn
        }
        None => {
            let login = connect
                .login
                .as_ref()
                .map(|l| (l.username.as_str(), l.password.as_str()));
            match broker.auth.load().authenticate(login) {
                Ok(identity) => identity,
                Err(denied) => {
                    let code = match denied {
                        ConnectDenied::BadCredentials => ConnectReturnCode::BadUserNamePassword,
                        ConnectDenied::AnonymousNotAllowed => ConnectReturnCode::NotAuthorized,
                    };
                    warn!(addr = %peer, user = ?login.map(|l| l.0), "CONNECT refused: {denied:?}");
                    Metrics::bump(&broker.metrics.connect_refused_total);
                    let attempted_id = if connect.client_id.is_empty() { "<unknown>" } else { &connect.client_id };
                    let user = login.map(|l| l.0).unwrap_or("<anonymous>");
                    publish_client_event(&broker, attempted_id, &format!("auth_fail addr={peer} user={user} reason={denied:?}")).await;
                    conn.write_packet(&Packet::ConnAck(ConnAck::new(code, false))).await?;
                    return Ok(());
                }
            }
        }
    };

    // A persistent session needs a stable name; an empty client id gets a
    // generated one and is forced clean (MQTT-3.1.3-7).
    let (client_id, clean) = if connect.client_id.is_empty() {
        (format!("entmoot-anon-{}", nanos_id()), true)
    } else {
        (connect.client_id.clone(), connect.clean_session)
    };

    if crate::churn::Verdict::Quarantined == broker.churn.admit(&client_id) {
        warn!(client = %client_id, addr = %peer, "CONNECT refused: reconnecting too often, quarantined");
        Metrics::bump(&broker.metrics.churn_quarantined_total);
        conn.write_packet(&Packet::ConnAck(ConnAck::new(ConnectReturnCode::ServiceUnavailable, false)))
            .await?;
        return Ok(());
    }

    Metrics::bump(&broker.metrics.connections_total);

    let (reader, writer_tx, writer_task) = conn.split();
    let kicked = Arc::new(Notify::new());
    let attach = broker.registry.attach(&client_id, &identity, clean, writer_tx.clone(), kicked.clone());

    send(
        &writer_tx,
        Packet::ConnAck(ConnAck::new(ConnectReturnCode::Success, attach.session_present)),
    )
    .await?;
    info!(
        client = %client_id,
        addr = %peer,
        user = %if identity.is_empty() { "<anonymous>" } else { &identity },
        clean,
        resumed = attach.session_present,
        keep_alive = connect.keep_alive,
        "client connected"
    );
    publish_client_event(&broker, &client_id, &format!("connect addr={peer} clean={clean}")).await;

    // Offline backlog drains first, before any live traffic (all QoS 1: only
    // QoS 1 subscriptions queue while offline).
    for (t, payload) in attach.backlog {
        let mut p = Publish::new(&t, QoS::AtLeastOnce, payload);
        p.pkid = attach.session.next_pkid();
        send(&writer_tx, Packet::Publish(p)).await?;
        Metrics::bump(&broker.metrics.messages_out_total);
    }

    let mut client = Client {
        rate: RateLimiter::new(broker.cfg.max_publish_rate),
        session: attach.session.clone(),
        broker: broker.clone(),
        id: client_id,
        identity,
        keep_alive: connect.keep_alive,
        will: connect.last_will.clone(),
        inflight_qos2: HashSet::new(),
    };

    let result = client.run(reader, writer_tx, writer_task, kicked).await;

    // Abnormal termination (error or keep-alive timeout) fires the Last Will.
    if let Err(ref e) = result {
        debug!(client = %client.id, "abnormal disconnect: {e:#}");
        client.fire_will().await;
    }
    broker.registry.detach(&attach.session, attach.epoch, clean);
    info!(client = %client.id, "client disconnected");
    let reason = if result.is_ok() { "clean" } else { "abnormal" };
    publish_client_event(&broker, &client.id, &format!("disconnect reason={reason}")).await;
    result
}

/// Emit a client lifecycle or audit event onto `$meta/clients/<node-id>/<id>`
/// (RESILIENCE_ROADMAP.md workstream 6, extended per ENTERPRISE_ROADMAP.md's
/// audit-logging item): connect/subscribe/unsubscribe/disconnect give a
/// visualizer a way to key client liveness off actual MQTT session activity
/// — carried over Zenoh's own session keepalives — rather than guessing from
/// tunnel/link state, which says nothing about whether a given MQTT client
/// is still there; auth_fail/publish_denied/subscribe_denied/will_denied
/// give a SIEM-facing process watching this same bus a live audit stream for
/// free, no separate mechanism needed (structured `tracing::warn!` logs
/// carry the same denials locally, per node, for anyone tailing logs
/// instead). Routed through the normal mesh-wide pub/sub path, like `$SYS`
/// and `$meta/<topic>` staleness: any session subscribed to
/// `$meta/clients/#` sees it, gated by the same ACLs as everything else.
/// Not retained: these are events, not current state.
async fn publish_client_event(broker: &Broker, client_id: &str, event: &str) {
    let ke = topic::meta_keyexpr(&format!("clients/{}/{client_id}", broker.cfg.id), &broker.cfg.scope);
    if let Err(e) = broker.session.put(&ke, event.as_bytes().to_vec()).await {
        warn!(client = client_id, "client meta event publish failed: {e}");
    }
}

struct Client {
    broker: Arc<Broker>,
    session: Arc<SessionState>,
    id: String,
    /// Authenticated username or certificate CN; empty = anonymous.
    identity: String,
    keep_alive: u16,
    will: Option<LastWill>,
    rate: RateLimiter,
    /// inbound QoS 2 packet ids awaiting PUBREL
    inflight_qos2: HashSet<u16>,
}

impl Client {
    /// Ok(()) = clean DISCONNECT or peer closed after clean traffic;
    /// Err = protocol violation / IO error / keep-alive expiry / takeover
    /// (fires the will).
    async fn run<S>(
        &mut self,
        mut reader: PacketReader<S>,
        writer_tx: Writer,
        writer_task: JoinHandle<()>,
        kicked: Arc<Notify>,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        // MQTT-3.1.2-24: server grants the client 1.5x the keep-alive interval.
        let idle_limit = if self.keep_alive == 0 {
            Duration::from_secs(u64::MAX / 4)
        } else {
            Duration::from_millis(self.keep_alive as u64 * 1500)
        };

        let result = loop {
            let packet = tokio::select! {
                read = tokio::time::timeout(idle_limit, reader.read_packet()) => match read {
                    Ok(Ok(p)) => p,
                    Ok(Err(e)) => break Err(e),
                    Err(_) => break Err(anyhow!("keep-alive expired ({}s)", self.keep_alive)),
                },
                _ = kicked.notified() => {
                    let reason = self
                        .session
                        .take_kick_reason()
                        .unwrap_or(crate::session::KICK_TAKEOVER);
                    break Err(anyhow!(reason));
                }
            };
            match packet {
                Packet::Publish(p) => {
                    if let Err(e) = self.handle_publish(p, &writer_tx).await {
                        break Err(e);
                    }
                }
                Packet::PubRel(pubrel) => {
                    self.inflight_qos2.remove(&pubrel.pkid);
                    send(&writer_tx, Packet::PubComp(PubComp::new(pubrel.pkid))).await?;
                }
                Packet::Subscribe(sub) => {
                    let mut codes = Vec::with_capacity(sub.filters.len());
                    let mut granted = Vec::new();
                    for f in &sub.filters {
                        let code = self.subscribe(&f.path, f.qos).await;
                        if let SubscribeReasonCode::Success(qos) = code {
                            granted.push((f.path.clone(), qos));
                        }
                        codes.push(code);
                    }
                    send(&writer_tx, Packet::SubAck(SubAck::new(sub.pkid, codes))).await?;
                    // MQTT-3.3.1-8: retained messages follow the SUBACK, with
                    // the retain flag set.
                    for (filter, qos) in granted {
                        self.deliver_retained(&filter, qos, &writer_tx).await?;
                    }
                }
                Packet::Unsubscribe(unsub) => {
                    for t in &unsub.topics {
                        self.session.remove_sub(t);
                        publish_client_event(&self.broker, &self.id, &format!("unsubscribe filter={t}")).await;
                    }
                    send(&writer_tx, Packet::UnsubAck(UnsubAck::new(unsub.pkid))).await?;
                }
                Packet::PingReq => send(&writer_tx, Packet::PingResp).await?,
                Packet::Disconnect => {
                    // Clean shutdown: MQTT-3.14.4-3, the will is discarded.
                    self.will = None;
                    break Ok(());
                }
                // Acks for our outbound QoS 1 traffic; no redelivery tracking yet.
                Packet::PubAck(_) | Packet::PubRec(_) | Packet::PubComp(_) => {}
                Packet::Connect(_) => break Err(anyhow!("duplicate CONNECT")),
                other => break Err(anyhow!("unexpected packet {other:?}")),
            }
        };

        // The session keeps a sender clone (its sink) until detach, so the
        // writer's channel never drains naturally — abort it instead.
        drop(writer_tx);
        writer_task.abort();
        result
    }

    async fn handle_publish(&mut self, p: Publish, writer_tx: &Writer) -> Result<()> {
        if !self.rate.allow() {
            Metrics::bump(&self.broker.metrics.rate_limit_disconnects_total);
            bail!(
                "publish rate limit exceeded ({}/s), disconnecting",
                self.broker.cfg.max_publish_rate
            );
        }
        let ke = topic::topic_to_keyexpr(&p.topic, &self.broker.cfg.scope)
            .map_err(|e| anyhow!("invalid publish topic {:?}: {e}", p.topic))?;

        if !self.broker.acl.load().may_publish(&self.identity, &p.topic) {
            warn!(client = %self.id, user = %self.identity, topic = %p.topic,
                  "publish denied by ACL, dropping");
            Metrics::bump(&self.broker.metrics.publish_denied_total);
            publish_client_event(&self.broker, &self.id, &format!("publish_denied topic={:?}", p.topic)).await;
        } else if let SchemaVerdict::Fail(action) = self.broker.schema.load().check(&p.topic, &p.payload) {
            Metrics::bump(&self.broker.metrics.schema_denied_total);
            match action {
                // Same reasoning as an ACL-denied publish: acked anyway
                // below, since stalling would just make the device retry
                // forever and v3.1.1 has no error ack to tell it otherwise.
                SchemaFailAction::Drop => {
                    warn!(client = %self.id, topic = %p.topic, "publish failed schema validation, dropping");
                }
                SchemaFailAction::Disconnect => {
                    bail!("publish on {:?} failed schema validation, disconnecting", p.topic);
                }
            }
        } else {
            self.broker
                .session
                .put(&ke, p.payload.to_vec())
                .await
                .map_err(|e| anyhow!("zenoh put on {ke}: {e}"))?;
            Metrics::bump(&self.broker.metrics.messages_in_total);
            if p.retain {
                let rke = topic::retained_keyexpr(&p.topic, &self.broker.cfg.scope);
                // MQTT-3.3.1-10/11: an empty retained payload clears the slot.
                let res = if p.payload.is_empty() {
                    self.broker.session.delete(&rke).await
                } else {
                    let envelope = crate::retained::encode_envelope(&p.payload, SystemTime::now());
                    self.broker.session.put(&rke, envelope).await
                };
                res.map_err(|e| anyhow!("retained write on {rke}: {e}"))?;
            }
        }

        // Ack even when the ACL dropped the message: v3.1.1 has no error ack,
        // and stalling the ack would just make the device retry forever.
        match p.qos {
            QoS::AtMostOnce => {}
            QoS::AtLeastOnce => send(writer_tx, Packet::PubAck(PubAck::new(p.pkid))).await?,
            QoS::ExactlyOnce => {
                self.inflight_qos2.insert(p.pkid);
                send(writer_tx, Packet::PubRec(PubRec::new(p.pkid))).await?;
            }
        }
        Ok(())
    }

    async fn subscribe(&mut self, filter: &str, req_qos: QoS) -> SubscribeReasonCode {
        if let Err(e) = topic::filter_to_keyexpr(filter, &self.broker.cfg.scope) {
            warn!(client = %self.id, filter, "rejecting subscription: {e}");
            Metrics::bump(&self.broker.metrics.subscribe_denied_total);
            return SubscribeReasonCode::Failure;
        }
        if !self.broker.acl.load().may_subscribe(&self.identity, filter) {
            warn!(client = %self.id, user = %self.identity, filter,
                  "subscription denied by ACL");
            Metrics::bump(&self.broker.metrics.subscribe_denied_total);
            publish_client_event(&self.broker, &self.id, &format!("subscribe_denied filter={filter:?}")).await;
            return SubscribeReasonCode::Failure;
        }

        // At most QoS 1 is granted (no exactly-once tracking across the mesh).
        let granted = match req_qos {
            QoS::AtMostOnce => QoS::AtMostOnce,
            _ => QoS::AtLeastOnce,
        };

        // The forwarding task belongs to the session (not the connection) and
        // survives a disconnect when the session is persistent; it is also
        // reinstated directly at startup by `session::rehydrate` from what was
        // persisted here, without waiting for the client to reconnect.
        if let Err(e) = activate_subscription(&self.broker, &self.session, filter, granted).await {
            warn!(client = %self.id, filter, "zenoh subscriber failed: {e}");
            return SubscribeReasonCode::Failure;
        }
        Metrics::bump(&self.broker.metrics.subscribes_total);
        publish_client_event(&self.broker, &self.id, &format!("subscribe filter={filter} qos={granted:?}")).await;
        SubscribeReasonCode::Success(granted)
    }

    async fn deliver_retained(&self, filter: &str, qos: QoS, writer_tx: &Writer) -> Result<()> {
        let matches = self.broker.retained.matching_cached(filter).await;
        for (topic_name, payload, written_at) in matches.iter() {
            let mut p = Publish::new(topic_name, qos, payload.clone());
            p.retain = true;
            if qos != QoS::AtMostOnce {
                p.pkid = self.session.next_pkid();
            }
            send(writer_tx, Packet::Publish(p)).await?;
            Metrics::bump(&self.broker.metrics.messages_out_total);
            self.flag_if_stale(topic_name, *written_at).await;
        }
        Ok(())
    }

    /// During a partition heal a retained value may be correct-but-old rather
    /// than current; if this topic's age exceeds its staleness bound, tell
    /// anyone listening on `$meta/<topic>` rather than let the plain retained
    /// delivery imply freshness. Routed through the normal mesh-wide pub/sub
    /// path (like `$SYS`), so it reaches every subscribed session, not just
    /// this one connection, and only those that actually asked for it.
    async fn flag_if_stale(&self, topic_name: &str, written_at: SystemTime) {
        let bound = self.broker.staleness.load().bound_secs(topic_name);
        if bound == 0 {
            return;
        }
        let age = SystemTime::now().duration_since(written_at).unwrap_or_default();
        if age < Duration::from_secs(bound) {
            return;
        }
        let meta_ke = topic::meta_keyexpr(topic_name, &self.broker.cfg.scope);
        let msg = format!("stale=true age_secs={} bound_secs={bound}", age.as_secs());
        if let Err(e) = self.broker.session.put(&meta_ke, msg.into_bytes()).await {
            warn!(topic = topic_name, "staleness meta publish failed: {e}");
        }
        Metrics::bump(&self.broker.metrics.stale_retained_total);
    }

    async fn fire_will(&mut self) {
        let Some(will) = self.will.take() else { return };
        if !self.broker.acl.load().may_publish(&self.identity, &will.topic) {
            warn!(client = %self.id, topic = %will.topic, "will denied by ACL, dropped");
            publish_client_event(&self.broker, &self.id, &format!("will_denied topic={:?}", will.topic)).await;
            return;
        }
        match topic::topic_to_keyexpr(&will.topic, &self.broker.cfg.scope) {
            Ok(ke) => {
                if let Err(e) = self.broker.session.put(&ke, will.message.to_vec()).await {
                    warn!(client = %self.id, "failed to publish will on {ke}: {e}");
                } else {
                    info!(client = %self.id, topic = %will.topic, "published last will");
                }
                if will.retain && !will.message.is_empty() {
                    let rke = topic::retained_keyexpr(&will.topic, &self.broker.cfg.scope);
                    let envelope = crate::retained::encode_envelope(&will.message, SystemTime::now());
                    self.broker.session.put(&rke, envelope).await.ok();
                }
            }
            Err(e) => warn!(client = %self.id, "will topic invalid: {e}"),
        }
    }
}

/// Token bucket: capacity = one second's worth of the configured rate.
struct RateLimiter {
    rate: f64,
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    fn new(rate_per_sec: u32) -> Self {
        Self { rate: rate_per_sec as f64, tokens: rate_per_sec as f64, last: Instant::now() }
    }

    fn allow(&mut self) -> bool {
        if self.rate == 0.0 {
            return true; // unlimited
        }
        let now = Instant::now();
        self.tokens = (self.tokens + self.rate * now.duration_since(self.last).as_secs_f64())
            .min(self.rate);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

type Writer = mpsc::Sender<Packet>;

async fn send(tx: &Writer, packet: Packet) -> Result<()> {
    tx.send(packet).await.map_err(|_| anyhow!("writer closed"))
}

fn nanos_id() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

/// Framed MQTT packet IO over any byte stream (TCP or TLS).
struct MqttConn<S> {
    stream: S,
    buf: BytesMut,
    max_packet_size: usize,
}

impl<S> MqttConn<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn new(stream: S, max_packet_size: usize) -> Self {
        Self { stream, buf: BytesMut::with_capacity(4096), max_packet_size }
    }

    async fn read_packet(&mut self) -> Result<Packet> {
        read_from(&mut self.stream, &mut self.buf, self.max_packet_size).await
    }

    async fn write_packet(&mut self, packet: &Packet) -> Result<()> {
        let mut out = BytesMut::new();
        encode(packet, &mut out)?;
        self.stream.write_all(&out).await.context("socket write")
    }

    /// Split into a reading half and a channel-fed writer task.
    fn split(self) -> (PacketReader<S>, Writer, JoinHandle<()>) {
        let (read_half, mut write_half) = tokio::io::split(self.stream);
        let (tx, mut rx) = mpsc::channel::<Packet>(OUTBOUND_QUEUE);
        let writer_task = tokio::spawn(async move {
            let mut out = BytesMut::with_capacity(4096);
            while let Some(packet) = rx.recv().await {
                out.clear();
                if encode(&packet, &mut out).is_err() {
                    continue;
                }
                if write_half.write_all(&out).await.is_err() {
                    break;
                }
            }
            write_half.shutdown().await.ok();
        });
        let reader = PacketReader {
            stream: read_half,
            buf: self.buf,
            max_packet_size: self.max_packet_size,
        };
        (reader, tx, writer_task)
    }
}

struct PacketReader<S> {
    stream: ReadHalf<S>,
    buf: BytesMut,
    max_packet_size: usize,
}

impl<S: AsyncRead + AsyncWrite + Unpin> PacketReader<S> {
    async fn read_packet(&mut self) -> Result<Packet> {
        read_from(&mut self.stream, &mut self.buf, self.max_packet_size).await
    }
}

async fn read_from<S: AsyncReadExt + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
    max_packet_size: usize,
) -> Result<Packet> {
    loop {
        match v4::read(buf, max_packet_size) {
            Ok(packet) => return Ok(packet),
            Err(mqttbytes::Error::InsufficientBytes(_)) => {
                if stream.read_buf(buf).await.context("socket read")? == 0 {
                    bail!("connection closed by peer");
                }
            }
            Err(e) => bail!("malformed MQTT packet: {e}"),
        }
    }
}

fn encode(packet: &Packet, out: &mut BytesMut) -> Result<()> {
    let res = match packet {
        Packet::ConnAck(p) => p.write(out),
        Packet::Publish(p) => p.write(out),
        Packet::PubAck(p) => p.write(out),
        Packet::PubRec(p) => p.write(out),
        Packet::PubComp(p) => p.write(out),
        Packet::SubAck(p) => p.write(out),
        Packet::UnsubAck(p) => p.write(out),
        Packet::PingResp => PingResp.write(out),
        other => bail!("encode not supported for {other:?}"),
    };
    res.map_err(|e| anyhow!("encode: {e}"))?;
    Ok(())
}
