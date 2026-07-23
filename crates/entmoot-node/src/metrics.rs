//! Node counters and a minimal Prometheus text-format endpoint.
//!
//! The endpoint is a deliberately tiny hand-rolled HTTP/1.1 responder (no
//! hyper): every request gets the metrics page and the connection is closed.
//! Bind it to an operations network, not the plant network.

use crate::Broker;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::warn;

#[derive(Default)]
pub struct Metrics {
    pub connections_total: AtomicU64,
    pub connect_refused_total: AtomicU64,
    pub connect_shed_total: AtomicU64,
    pub churn_quarantined_total: AtomicU64,
    pub quota_refused_total: AtomicU64,
    pub messages_in_total: AtomicU64,
    pub messages_out_total: AtomicU64,
    pub messages_queued_total: AtomicU64,
    pub publish_denied_total: AtomicU64,
    pub subscribe_denied_total: AtomicU64,
    pub schema_denied_total: AtomicU64,
    pub subscribes_total: AtomicU64,
    pub stale_retained_total: AtomicU64,
    pub rate_limit_disconnects_total: AtomicU64,
    pub slow_consumer_evictions_total: AtomicU64,
}

impl Metrics {
    pub fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn render(broker: &Broker) -> String {
    let m = &broker.metrics;
    let (sessions, offline, queued, dropped) = broker.registry.stats();
    let mut out = String::with_capacity(1024);
    let mut metric = |name: &str, kind: &str, help: &str, value: u64| {
        let _ = writeln!(out, "# HELP entmoot_{name} {help}");
        let _ = writeln!(out, "# TYPE entmoot_{name} {kind}");
        let _ = writeln!(out, "entmoot_{name}{{node=\"{}\"}} {value}", broker.cfg.id);
    };
    let c = |a: &AtomicU64| a.load(Ordering::Relaxed);
    metric("connections_current", "gauge", "Open MQTT connections", broker.connections() as u64);
    metric("connections_total", "counter", "MQTT connections accepted since start", c(&m.connections_total));
    metric("connect_refused_total", "counter", "CONNECTs refused (auth)", c(&m.connect_refused_total));
    metric("connect_shed_total", "counter", "CONNECTs shed by admission control (reconnect-storm protection)", c(&m.connect_shed_total));
    metric("churn_quarantined_total", "counter", "CONNECTs refused because that client id was reconnecting too often", c(&m.churn_quarantined_total));
    metric("quota_refused_total", "counter", "CONNECTs refused because that identity was already at its per-identity connection quota", c(&m.quota_refused_total));
    metric("messages_in_total", "counter", "PUBLISH packets accepted from clients", c(&m.messages_in_total));
    metric("messages_out_total", "counter", "PUBLISH packets delivered to clients", c(&m.messages_out_total));
    metric("messages_queued_total", "counter", "Messages queued for offline sessions", c(&m.messages_queued_total));
    metric("publish_denied_total", "counter", "Publishes dropped by ACL", c(&m.publish_denied_total));
    metric("schema_denied_total", "counter", "Publishes that failed data-validation schema checks", c(&m.schema_denied_total));
    metric("subscribe_denied_total", "counter", "Subscriptions refused by ACL or validation", c(&m.subscribe_denied_total));
    metric("subscribes_total", "counter", "Subscriptions granted", c(&m.subscribes_total));
    metric("retained_scans_total", "counter", "Retained-store scans performed (denominator for the reconnect-storm coalescing ratio: scans / subscribes_total)", broker.retained.scan_count());
    metric("stale_retained_total", "counter", "Retained deliveries flagged stale on $meta/<topic> (partition-heal staleness bound exceeded)", c(&m.stale_retained_total));
    metric("rate_limit_disconnects_total", "counter", "Clients disconnected for exceeding the publish rate", c(&m.rate_limit_disconnects_total));
    metric("slow_consumer_evictions_total", "counter", "Clients evicted because their outbound queue stayed full", c(&m.slow_consumer_evictions_total));
    metric("retained_messages", "gauge", "Retained messages replicated on this node", broker.retained.len() as u64);
    metric("sessions", "gauge", "Known MQTT sessions (connected + offline persistent)", sessions as u64);
    metric("sessions_offline", "gauge", "Offline persistent sessions", offline as u64);
    metric("session_queue_depth", "gauge", "QoS 1 messages currently queued for offline sessions", queued as u64);
    metric("session_queue_dropped_total", "counter", "Messages dropped from full offline queues", dropped);
    out
}

/// Periodically publish node stats on `$SYS/broker/<node-id>/...`. Values go
/// through the same zenoh path as everything else (via the `@sys` verbatim
/// keyspace), so any node's stats are visible mesh-wide — but only to clients
/// that explicitly subscribe under `$SYS` (never to `#`, per MQTT-4.7.2-1),
/// and no client can publish there.
pub async fn sys_publish(broker: Arc<Broker>, interval: Duration) {
    let started = std::time::Instant::now();
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let m = &broker.metrics;
        let c = |a: &AtomicU64| a.load(Ordering::Relaxed).to_string();
        let (sessions, offline, _, _) = broker.registry.stats();
        let stats = [
            ("clients/connected", broker.connections().to_string()),
            ("messages/received", c(&m.messages_in_total)),
            ("messages/sent", c(&m.messages_out_total)),
            ("retained/count", broker.retained.len().to_string()),
            ("sessions/count", sessions.to_string()),
            ("sessions/offline", offline.to_string()),
            ("uptime/seconds", started.elapsed().as_secs().to_string()),
            ("version", env!("CARGO_PKG_VERSION").to_string()),
        ];
        for (path, value) in stats {
            let suffix = format!("broker/{}/{}", broker.cfg.id, path);
            let ke = entmoot_core::topic::sys_keyexpr(&suffix, &broker.cfg.scope);
            if let Err(e) = broker.session.put(&ke, value.into_bytes()).await {
                warn!("$SYS publish on {ke} failed: {e}");
            }
        }
    }
}

pub async fn serve(listener: TcpListener, broker: Arc<Broker>) {
    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                warn!("metrics accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let broker = broker.clone();
        tokio::spawn(async move {
            // Read (and ignore) the request; any path gets the metrics page.
            let mut buf = [0u8; 1024];
            let _ = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;
            let body = render(&broker);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes()).await;
        });
    }
}
