//! Persistent MQTT sessions (cleanSession=0).
//!
//! A session owns its subscriptions: the zenoh subscriber tasks live in the
//! session, not the connection, so they keep collecting matching messages
//! after the TCP connection dies. While a client is offline, QoS 1 traffic is
//! queued (bounded, drop-oldest); on reconnect the backlog is drained first.
//!
//! Reconnecting with the same client id takes the session over (MQTT-3.1.4-2):
//! the old connection is kicked, and a connection "epoch" makes sure the loser
//! of that race cannot detach the winner's sink.
//!
//! Slow-consumer eviction: a client whose outbound queue stays full past the
//! configured grace is kicked rather than allowed to stall its mesh
//! subscriber tasks forever; delivery falls through to the offline path.

use crate::metrics::Metrics;
use bytes::Bytes;
use mqttbytes::v4::{Packet, Publish};
use mqttbytes::QoS;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

const QUEUE_MAGIC: &[u8] = b"ENTMOOT-Q1\n";

pub struct SessionRegistry {
    sessions: Mutex<HashMap<String, Arc<SessionState>>>,
    max_queue: usize,
    slow_grace: Option<Duration>,
    queue_dir: Option<PathBuf>,
}

pub struct AttachOutcome {
    pub session: Arc<SessionState>,
    /// MQTT-3.2.2-2: true iff cleanSession=0 and stored state existed.
    pub session_present: bool,
    /// Identifies this connection within the session; pass back to `detach`.
    pub epoch: u64,
    /// QoS 1 messages queued while the client was offline.
    pub backlog: Vec<(String, Bytes)>,
}

impl SessionRegistry {
    /// `slow_grace = None` disables slow-consumer eviction (block forever).
    pub fn new(max_queue: usize, slow_grace: Option<Duration>, queue_dir: Option<PathBuf>) -> Self {
        Self { sessions: Mutex::new(HashMap::new()), max_queue, slow_grace, queue_dir }
    }

    pub fn attach(
        &self,
        client_id: &str,
        clean: bool,
        sink: mpsc::Sender<Packet>,
        kick: Arc<Notify>,
    ) -> AttachOutcome {
        let session;
        let session_present;
        let queue_path = self.queue_path(client_id);
        {
            let mut map = self.sessions.lock().unwrap();
            match map.get(client_id).cloned() {
                Some(existing) if !clean => {
                    session = existing;
                    session_present = true;
                }
                Some(existing) => {
                    // cleanSession=1 discards any stored state (MQTT-3.1.2-6).
                    existing.kick_current();
                    existing.abort_subs();
                    if let Some(path) = &queue_path {
                        remove_queue_file(path);
                    }
                    session = Arc::new(SessionState::new(
                        client_id,
                        self.max_queue,
                        self.slow_grace,
                        None,
                    ));
                    session_present = false;
                }
                None => {
                    if clean {
                        if let Some(path) = &queue_path {
                            remove_queue_file(path);
                        }
                    }
                    session_present = !clean && queue_path.as_ref().is_some_and(|p| p.exists());
                    session = Arc::new(SessionState::new(
                        client_id,
                        self.max_queue,
                        self.slow_grace,
                        if clean { None } else { queue_path.clone() },
                    ));
                }
            }
            map.insert(client_id.to_string(), session.clone());
        }
        let (epoch, backlog) = session.attach_conn(sink, kick);
        if session_present {
            info!(client = %client_id, backlog = backlog.len(), "persistent session resumed");
        }
        AttachOutcome { session, session_present, epoch, backlog }
    }

    /// Called when a connection ends. `clean` sessions are destroyed;
    /// persistent ones stay registered, queueing, until expiry.
    pub fn detach(&self, session: &Arc<SessionState>, epoch: u64, clean: bool) {
        if !session.detach_conn(epoch) {
            return; // a newer connection took the session over
        }
        if clean {
            session.abort_subs();
            session.discard_queue_file();
            // Only remove our own entry: a reconnect may already have replaced
            // it with a fresh session under the same client id.
            let mut map = self.sessions.lock().unwrap();
            if map.get(&session.client_id).is_some_and(|cur| Arc::ptr_eq(cur, session)) {
                map.remove(&session.client_id);
            }
        }
    }

    /// Drop offline sessions older than `expiry`.
    pub fn sweep(&self, expiry: Duration) {
        let mut expired = Vec::new();
        self.sessions.lock().unwrap().retain(|_, s| {
            if s.expired(expiry) {
                expired.push(s.clone());
                false
            } else {
                true
            }
        });
        for s in expired {
            info!(client = %s.client_id, "offline session expired, discarding");
            s.abort_subs();
            s.discard_queue_file();
        }
    }

    /// (sessions, offline, queued messages, total dropped) for metrics.
    pub fn stats(&self) -> (usize, usize, usize, u64) {
        let map = self.sessions.lock().unwrap();
        let mut offline = 0;
        let mut queued = 0;
        let mut dropped = 0;
        for s in map.values() {
            let inner = s.inner.lock().unwrap();
            if inner.sink.is_none() {
                offline += 1;
            }
            queued += inner.queue.len();
            dropped += inner.dropped;
        }
        (map.len(), offline, queued, dropped)
    }

    fn queue_path(&self, client_id: &str) -> Option<PathBuf> {
        self.queue_dir.as_ref().map(|dir| dir.join(format!("{}.queue", hex_name(client_id))))
    }
}

pub struct SessionState {
    pub client_id: String,
    pkid: AtomicU16,
    max_queue: usize,
    slow_grace: Option<Duration>,
    queue_path: Option<PathBuf>,
    inner: Mutex<Inner>,
}

struct Inner {
    epoch: u64,
    sink: Option<mpsc::Sender<Packet>>,
    kick: Option<Arc<Notify>>,
    /// Why the current connection was kicked; read by the kicked connection
    /// to log/report the real cause (takeover vs slow-consumer eviction).
    kick_reason: Option<&'static str>,
    subs: HashMap<String, SubEntry>,
    queue: VecDeque<(String, Bytes)>,
    dropped: u64,
    offline_since: Option<Instant>,
}

struct SubEntry {
    task: JoinHandle<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    Sent,
    Queued,
    Dropped,
}

pub const KICK_TAKEOVER: &str = "session taken over by a new connection";
pub const KICK_SLOW: &str = "slow consumer: outbound queue stayed full past the grace period";

impl SessionState {
    fn new(
        client_id: &str,
        max_queue: usize,
        slow_grace: Option<Duration>,
        queue_path: Option<PathBuf>,
    ) -> Self {
        let queue = queue_path
            .as_ref()
            .and_then(|path| match load_queue(path, max_queue) {
                Ok(queue) => Some(queue),
                Err(e) => {
                    warn!(client = %client_id, file = %path.display(), "ignoring offline queue file: {e}");
                    None
                }
            })
            .unwrap_or_default();
        Self {
            client_id: client_id.to_string(),
            pkid: AtomicU16::new(1),
            max_queue,
            slow_grace,
            queue_path,
            inner: Mutex::new(Inner {
                epoch: 0,
                sink: None,
                kick: None,
                kick_reason: None,
                subs: HashMap::new(),
                queue,
                dropped: 0,
                offline_since: None,
            }),
        }
    }

    /// Why the current connection was kicked (consumed on read).
    pub fn take_kick_reason(&self) -> Option<&'static str> {
        self.inner.lock().unwrap().kick_reason.take()
    }

    pub fn next_pkid(&self) -> u16 {
        loop {
            let id = self.pkid.fetch_add(1, Ordering::Relaxed);
            if id != 0 {
                return id; // pkid 0 is invalid; skip it on wrap
            }
        }
    }

    fn attach_conn(&self, sink: mpsc::Sender<Packet>, kick: Arc<Notify>) -> (u64, Vec<(String, Bytes)>) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(old_kick) = inner.kick.take() {
            inner.kick_reason = Some(KICK_TAKEOVER);
            old_kick.notify_waiters(); // takeover: boot the previous connection
        }
        inner.epoch += 1;
        inner.sink = Some(sink);
        inner.kick = Some(kick);
        inner.offline_since = None;
        let epoch = inner.epoch;
        let backlog = inner.queue.drain(..).collect();
        drop(inner);
        self.discard_queue_file();
        (epoch, backlog)
    }

    /// Returns false if this connection was already superseded.
    fn detach_conn(&self, epoch: u64) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.epoch != epoch {
            return false;
        }
        inner.sink = None;
        inner.kick = None;
        inner.offline_since = Some(Instant::now());
        true
    }

    fn kick_current(&self) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(kick) = inner.kick.clone() {
            inner.kick_reason = Some(KICK_TAKEOVER);
            kick.notify_waiters();
        }
    }

    fn expired(&self, expiry: Duration) -> bool {
        self.inner
            .lock()
            .unwrap()
            .offline_since
            .map(|t| t.elapsed() > expiry)
            .unwrap_or(false)
    }

    /// Kick the connection behind `tx` for being a slow consumer. No-op if a
    /// newer connection already replaced it (racing deliveries may both time
    /// out on the same sink; only the first one evicts).
    fn evict_slow(&self, tx: &mpsc::Sender<Packet>, metrics: &Metrics) {
        let mut inner = self.inner.lock().unwrap();
        if !inner.sink.as_ref().is_some_and(|cur| cur.same_channel(tx)) {
            return;
        }
        info!(client = %self.client_id, "evicting slow consumer");
        Metrics::bump(&metrics.slow_consumer_evictions_total);
        inner.kick_reason = Some(KICK_SLOW);
        if let Some(kick) = &inner.kick {
            kick.notify_waiters();
        }
        inner.sink = None;
        inner.offline_since = Some(Instant::now());
    }

    pub fn insert_sub(&self, filter: String, task: JoinHandle<()>) {
        if let Some(old) = self.inner.lock().unwrap().subs.insert(filter, SubEntry { task }) {
            old.task.abort(); // MQTT-3.8.4-3: re-subscribe replaces
        }
    }

    pub fn remove_sub(&self, filter: &str) {
        if let Some(entry) = self.inner.lock().unwrap().subs.remove(filter) {
            entry.task.abort();
        }
    }

    pub fn abort_subs(&self) {
        for (_, entry) in self.inner.lock().unwrap().subs.drain() {
            entry.task.abort();
        }
    }

    fn discard_queue_file(&self) {
        if let Some(path) = &self.queue_path {
            remove_queue_file(path);
        }
    }

    fn persist_queue(&self, inner: &Inner) {
        let Some(path) = &self.queue_path else { return };
        if let Err(e) = save_queue(path, &inner.queue) {
            warn!(client = %self.client_id, file = %path.display(), "offline queue persistence failed: {e}");
        }
    }

    /// Deliver one message to this session: to the live connection if there is
    /// one, to the offline queue for QoS 1, or drop for offline QoS 0. A
    /// connection whose outbound queue stays full past the slow-consumer
    /// grace is evicted and delivery continues on the offline path.
    pub async fn deliver(
        self: &Arc<Self>,
        topic: &str,
        payload: Bytes,
        qos: QoS,
        metrics: &Metrics,
    ) -> Delivery {
        loop {
            let sink = self.inner.lock().unwrap().sink.clone();
            match sink {
                Some(tx) => {
                    let mut p = Publish::new(topic, qos, payload.clone());
                    if qos != QoS::AtMostOnce {
                        p.pkid = self.next_pkid();
                    }
                    let sent = match self.slow_grace {
                        Some(grace) => match tx.send_timeout(Packet::Publish(p), grace).await {
                            Ok(()) => true,
                            Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
                                self.evict_slow(&tx, metrics);
                                false
                            }
                            Err(mpsc::error::SendTimeoutError::Closed(_)) => false,
                        },
                        None => tx.send(Packet::Publish(p)).await.is_ok(),
                    };
                    if sent {
                        return Delivery::Sent;
                    }
                    // Writer gone (connection died mid-flight or was just
                    // evicted): clear the sink if nobody replaced it yet,
                    // then take the offline path.
                    let mut inner = self.inner.lock().unwrap();
                    if inner.sink.as_ref().is_some_and(|cur| cur.same_channel(&tx)) {
                        inner.sink = None;
                        inner.offline_since = Some(Instant::now());
                    }
                }
                None => {
                    if qos == QoS::AtMostOnce || self.max_queue == 0 {
                        return Delivery::Dropped;
                    }
                    let mut inner = self.inner.lock().unwrap();
                    if inner.sink.is_some() {
                        continue; // reconnected between checks; retry live path
                    }
                    if inner.queue.len() >= self.max_queue {
                        inner.queue.pop_front();
                        inner.dropped += 1;
                        debug!(client = %self.client_id, "offline queue full, dropped oldest");
                    }
                    inner.queue.push_back((topic.to_string(), payload));
                    self.persist_queue(&inner);
                    return Delivery::Queued;
                }
            }
        }
    }
}

fn remove_queue_file(path: &Path) {
    if let Err(e) = std::fs::remove_file(path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            warn!(file = %path.display(), "offline queue delete failed: {e}");
        }
    }
}

fn load_queue(path: &Path, max_queue: usize) -> anyhow::Result<VecDeque<(String, Bytes)>> {
    if max_queue == 0 {
        return Ok(VecDeque::new());
    }
    let data = std::fs::read(path)?;
    let body = data
        .strip_prefix(QUEUE_MAGIC)
        .ok_or_else(|| anyhow::anyhow!("not an entmoot offline queue"))?;
    let mut rest = body;
    let mut queue = VecDeque::new();
    while !rest.is_empty() {
        let topic = String::from_utf8(read_chunk(&mut rest)?)
            .map_err(|_| anyhow::anyhow!("bad topic in offline queue"))?;
        let payload = Bytes::from(read_chunk(&mut rest)?);
        if queue.len() >= max_queue {
            queue.pop_front();
        }
        queue.push_back((topic, payload));
    }
    Ok(queue)
}

fn save_queue(path: &Path, queue: &VecDeque<(String, Bytes)>) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut out = Vec::with_capacity(QUEUE_MAGIC.len() + queue.len() * 64);
    out.extend_from_slice(QUEUE_MAGIC);
    for (topic, payload) in queue {
        write_chunk(&mut out, topic.as_bytes())?;
        write_chunk(&mut out, payload)?;
    }
    let tmp = path.with_extension("queue.tmp");
    std::fs::write(&tmp, out)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

fn read_chunk(rest: &mut &[u8]) -> anyhow::Result<Vec<u8>> {
    let (len_bytes, tail) = rest
        .split_at_checked(4)
        .ok_or_else(|| anyhow::anyhow!("truncated offline queue"))?;
    let len = u32::from_be_bytes(len_bytes.try_into().unwrap()) as usize;
    let (chunk, tail) = tail
        .split_at_checked(len)
        .ok_or_else(|| anyhow::anyhow!("truncated offline queue"))?;
    *rest = tail;
    Ok(chunk.to_vec())
}

fn write_chunk(out: &mut Vec<u8>, chunk: &[u8]) -> anyhow::Result<()> {
    if chunk.len() > u32::MAX as usize {
        anyhow::bail!("offline queue chunk too large");
    }
    out.extend_from_slice(&(chunk.len() as u32).to_be_bytes());
    out.extend_from_slice(chunk);
    Ok(())
}

fn hex_name(s: &str) -> String {
    s.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}
