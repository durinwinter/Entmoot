//! Mesh-wide retained-message store.
//!
//! Retained publishes are written a second time under the internal
//! `[scope/]@retained/<topic>` keyspace (a zenoh `delete` clears them). Every
//! node keeps an in-memory replica fed by a subscriber on that keyspace, and
//! serves it via a queryable so a node that joins the mesh late can catch up
//! with one `get`. Client topics can never collide with the keyspace: levels
//! starting with '@' are rejected at validation.
//!
//! `matching` is a linear scan against the whole replica per SUBSCRIBE. In a
//! reconnect storm many clients share the same filter (`plant/#` is typical
//! in an industrial namespace); [`RetainedStore::matching_cached`] gives
//! those concurrent SUBSCRIBEs singleflight semantics via moka, so one scan
//! serves all of them instead of redoing the work (and the shared-lock
//! traffic) once per client.
//!
//! Every value in the keyspace carries its origin write time, so a node can
//! tell a client "this is what I have, and it's N seconds old" rather than
//! presenting a value that survived a partition as if it were current. The
//! timestamp travels as an 8-byte big-endian millis-since-epoch prefix on the
//! `@retained` payload itself ([`encode_envelope`]/[`decode_envelope`]) —
//! Zenoh's own per-sample timestamp isn't usable here because a queryable
//! reply to a late-joining node gets a fresh timestamp, not the original
//! one, and the public API gives no way to override that. This is wall-clock
//! (the writing node's `SystemTime::now()`), not Zenoh's HLC: it doesn't
//! correct for clock skew between nodes the way a true HLC comparison would,
//! but it needs no additional wire dependency and travels intact across
//! live replication, queryable catch-up, and our own disk snapshot alike.
//! This keyspace is Entmoot-internal (see module docs above), so the change
//! never touches the plain live keyspace non-MQTT consumers (e.g. Twilight
//! Bark) tap — those payloads stay opaque end-to-end exactly as documented.

use anyhow::{anyhow, Result};
use bytes::Bytes;
use entmoot_core::topic;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use zenoh::sample::SampleKind;
use zenoh::Session;

/// How long after startup to wait before pulling the retained snapshot from
/// peers (gives the explicit peer links time to establish).
const FETCH_DELAY: Duration = Duration::from_millis(750);

/// Prefix `payload` with its origin write time for the `@retained` keyspace
/// wire format. See the module docs for why this rides in the payload
/// instead of Zenoh's own sample timestamp.
pub(crate) fn encode_envelope(payload: &[u8], written_at: SystemTime) -> Vec<u8> {
    let millis = written_at.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
    let mut out = Vec::with_capacity(8 + payload.len());
    out.extend_from_slice(&millis.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Inverse of [`encode_envelope`]. `None` on a malformed (too-short) envelope.
pub(crate) fn decode_envelope(bytes: &[u8]) -> Option<(Bytes, SystemTime)> {
    let (ts_bytes, payload) = bytes.split_at_checked(8)?;
    let millis = u64::from_be_bytes(ts_bytes.try_into().unwrap());
    Some((Bytes::copy_from_slice(payload), UNIX_EPOCH + Duration::from_millis(millis)))
}

/// (topic, payload, origin write time) — one retained match.
type RetainedEntry = (String, Bytes, SystemTime);

pub struct RetainedStore {
    map: RwLock<HashMap<String, (Bytes, SystemTime)>>,
    dirty: std::sync::atomic::AtomicBool,
    /// Coalesces concurrent `matching(filter)` calls for the same filter
    /// (the reconnect-storm shape: many clients share a filter like
    /// `plant/#`) into one scan. Invalidated wholesale on every mutation, so
    /// it never serves a result older than the last insert/remove.
    match_cache: moka::future::Cache<String, Arc<Vec<RetainedEntry>>>,
    /// Count of actual underlying scans performed (cache misses). Compared
    /// against subscribe grants, this is the fan-out ratio the reconnect-storm
    /// coalescing is meant to shrink.
    scans: std::sync::atomic::AtomicU64,
}

impl Default for RetainedStore {
    fn default() -> Self {
        Self {
            map: RwLock::default(),
            dirty: std::sync::atomic::AtomicBool::default(),
            match_cache: moka::future::Cache::new(MATCH_CACHE_CAPACITY),
            scans: std::sync::atomic::AtomicU64::default(),
        }
    }
}

/// Snapshot format version 2: adds the origin-write timestamp per entry.
/// Version 1 snapshots fail the magic check and are ignored (logged, not
/// fatal) rather than silently misparsed — acceptable for pre-release state.
const SNAPSHOT_MAGIC: &[u8] = b"ENTMOOT-RET2\n";
const PERSIST_INTERVAL: Duration = Duration::from_secs(2);
/// Distinct subscription filters a node is expected to see concurrently
/// during a storm; a generous ceiling, not a hard limit (moka evicts LRU
/// beyond it, which only costs a re-scan, never correctness).
const MATCH_CACHE_CAPACITY: u64 = 10_000;

impl RetainedStore {
    pub fn insert(&self, topic_name: String, payload: Bytes, written_at: SystemTime) {
        self.map.write().unwrap().insert(topic_name, (payload, written_at));
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        self.match_cache.invalidate_all();
    }

    pub fn remove(&self, topic_name: &str) {
        self.map.write().unwrap().remove(topic_name);
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        self.match_cache.invalidate_all();
    }

    pub fn len(&self) -> usize {
        self.map.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn take_dirty(&self) -> bool {
        self.dirty.swap(false, std::sync::atomic::Ordering::Relaxed)
    }

    /// Merge a snapshot file into the store. Length-prefixed binary; retained
    /// payloads are opaque bytes, so no text encoding.
    pub fn load_snapshot(&self, path: &std::path::Path) -> Result<usize> {
        let data = std::fs::read(path)?;
        let body = data
            .strip_prefix(SNAPSHOT_MAGIC)
            .ok_or_else(|| anyhow!("{}: not an entmoot retained snapshot", path.display()))?;
        let mut rest = body;
        let mut count = 0usize;
        let read_chunk = |rest: &mut &[u8]| -> Result<Vec<u8>> {
            let (len_bytes, tail) = rest
                .split_at_checked(4)
                .ok_or_else(|| anyhow!("truncated snapshot"))?;
            let len = u32::from_be_bytes(len_bytes.try_into().unwrap()) as usize;
            let (chunk, tail) = tail
                .split_at_checked(len)
                .ok_or_else(|| anyhow!("truncated snapshot"))?;
            *rest = tail;
            Ok(chunk.to_vec())
        };
        while !rest.is_empty() {
            let topic_bytes = read_chunk(&mut rest)?;
            let payload = read_chunk(&mut rest)?;
            let (ts_bytes, tail) =
                rest.split_at_checked(8).ok_or_else(|| anyhow!("truncated snapshot"))?;
            let millis = u64::from_be_bytes(ts_bytes.try_into().unwrap());
            rest = tail;
            let t = String::from_utf8(topic_bytes).map_err(|_| anyhow!("bad topic in snapshot"))?;
            let written_at = UNIX_EPOCH + Duration::from_millis(millis);
            self.map.write().unwrap().insert(t, (Bytes::from(payload), written_at));
            count += 1;
        }
        Ok(count)
    }

    /// Atomically (write temp + rename) snapshot the store to disk.
    pub fn save_snapshot(&self, path: &std::path::Path) -> Result<()> {
        let mut out = Vec::with_capacity(4096);
        out.extend_from_slice(SNAPSHOT_MAGIC);
        for (t, payload, written_at) in self.snapshot() {
            out.extend_from_slice(&(t.len() as u32).to_be_bytes());
            out.extend_from_slice(t.as_bytes());
            out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            out.extend_from_slice(&payload);
            let millis = written_at.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
            out.extend_from_slice(&millis.to_be_bytes());
        }
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &out)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Retained messages matching an MQTT subscription filter, with each
    /// entry's origin write time.
    pub fn matching(&self, filter: &str) -> Vec<RetainedEntry> {
        self.scans.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.map
            .read()
            .unwrap()
            .iter()
            .filter(|(t, _)| topic::topic_matches(filter, t))
            .map(|(t, (p, ts))| (t.clone(), p.clone(), *ts))
            .collect()
    }

    /// Number of underlying scans performed since startup (i.e. cache misses
    /// on `matching_cached`, plus any direct `matching` calls).
    pub fn scan_count(&self) -> u64 {
        self.scans.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Same result as [`matching`](Self::matching), but concurrent calls for
    /// the same filter share one scan (moka `get_with` blocks concurrent
    /// misses on a key behind a single loader) — the coalescing that matters
    /// when a reconnect storm sends a wave of SUBSCRIBEs for the same filter
    /// at once.
    pub async fn matching_cached(&self, filter: &str) -> Arc<Vec<RetainedEntry>> {
        self.match_cache
            .get_with_by_ref(filter, async { Arc::new(self.matching(filter)) })
            .await
    }

    pub fn snapshot(&self) -> Vec<RetainedEntry> {
        self.map.read().unwrap().iter().map(|(t, (p, ts))| (t.clone(), p.clone(), *ts)).collect()
    }
}

/// Start the three background tasks that keep this node's replica in sync:
/// live updates (subscriber), serving late joiners (queryable), and catching
/// up ourselves (startup fetch). Returns handles so shutdown can abort them.
pub async fn wire(
    session: Session,
    store: std::sync::Arc<RetainedStore>,
    scope: String,
    persist_path: Option<std::path::PathBuf>,
) -> Result<Vec<JoinHandle<()>>> {
    let filter = topic::retained_filter(&scope);
    let mut tasks = Vec::new();

    // 0. Node-local durability: restore the last snapshot, then keep one
    // current (debounced, atomic-rename). Peer catch-up below still runs and
    // wins for anything fresher than the snapshot.
    if let Some(path) = persist_path {
        if path.exists() {
            match store.load_snapshot(&path) {
                Ok(n) => info!(count = n, file = %path.display(), "retained snapshot restored"),
                Err(e) => warn!("ignoring retained snapshot {}: {e}", path.display()),
            }
        }
        store.take_dirty(); // what we just loaded doesn't need re-saving
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(PERSIST_INTERVAL);
            loop {
                tick.tick().await;
                if store.take_dirty() {
                    if let Err(e) = store.save_snapshot(&path) {
                        warn!("retained snapshot write failed: {e}");
                    }
                }
            }
        }));
    }

    // 1. Live replication: every retained put/delete anywhere in the mesh.
    let sub = session
        .declare_subscriber(&filter)
        .await
        .map_err(|e| anyhow!("retained subscriber on {filter}: {e}"))?;
    {
        let store = store.clone();
        let scope = scope.clone();
        tasks.push(tokio::spawn(async move {
            while let Ok(sample) = sub.recv_async().await {
                let Some(t) = topic::retained_keyexpr_to_topic(sample.key_expr().as_str(), &scope)
                else {
                    continue;
                };
                match sample.kind() {
                    SampleKind::Put => {
                        let raw = sample.payload().to_bytes();
                        let Some((payload, written_at)) = decode_envelope(&raw) else {
                            warn!(topic = t, "malformed retained envelope, ignoring");
                            continue;
                        };
                        debug!(topic = t, bytes = payload.len(), "retained stored");
                        store.insert(t.to_string(), payload, written_at);
                    }
                    SampleKind::Delete => {
                        debug!(topic = t, "retained cleared");
                        store.remove(t);
                    }
                }
            }
        }));
    }

    // 2. Serve our replica to nodes that join later.
    let queryable = session
        .declare_queryable(&filter)
        .await
        .map_err(|e| anyhow!("retained queryable on {filter}: {e}"))?;
    {
        let store = store.clone();
        let scope = scope.clone();
        tasks.push(tokio::spawn(async move {
            while let Ok(query) = queryable.recv_async().await {
                for (t, payload, written_at) in store.snapshot() {
                    let ke = topic::retained_keyexpr(&t, &scope);
                    let envelope = encode_envelope(&payload, written_at);
                    if let Err(e) = query.reply(&ke, envelope).await {
                        warn!("retained query reply on {ke}: {e}");
                    }
                }
            }
        }));
    }

    // 3. Catch up from peers once our links are established.
    tasks.push(tokio::spawn(async move {
        tokio::time::sleep(FETCH_DELAY).await;
        let replies = match session.get(&filter).await {
            Ok(r) => r,
            Err(e) => {
                warn!("retained catch-up query failed: {e}");
                return;
            }
        };
        let mut fetched = 0usize;
        while let Ok(reply) = replies.recv_async().await {
            let Ok(sample) = reply.result() else { continue };
            let Some(t) = topic::retained_keyexpr_to_topic(sample.key_expr().as_str(), &scope)
            else {
                continue;
            };
            let raw = sample.payload().to_bytes();
            let Some((payload, written_at)) = decode_envelope(&raw) else {
                warn!(topic = t, "malformed retained envelope in catch-up reply, ignoring");
                continue;
            };
            store.insert(t.to_string(), payload, written_at);
            fetched += 1;
        }
        if fetched > 0 {
            debug!(count = fetched, "retained catch-up complete");
        }
    }));

    Ok(tasks)
}
