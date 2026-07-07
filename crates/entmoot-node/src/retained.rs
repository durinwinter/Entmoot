//! Mesh-wide retained-message store.
//!
//! Retained publishes are written a second time under the internal
//! `[scope/]@retained/<topic>` keyspace (a zenoh `delete` clears them). Every
//! node keeps an in-memory replica fed by a subscriber on that keyspace, and
//! serves it via a queryable so a node that joins the mesh late can catch up
//! with one `get`. Client topics can never collide with the keyspace: levels
//! starting with '@' are rejected at validation.

use anyhow::{anyhow, Result};
use bytes::Bytes;
use entmoot_core::topic;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use zenoh::sample::SampleKind;
use zenoh::Session;

/// How long after startup to wait before pulling the retained snapshot from
/// peers (gives the explicit peer links time to establish).
const FETCH_DELAY: Duration = Duration::from_millis(750);

#[derive(Default)]
pub struct RetainedStore {
    map: RwLock<HashMap<String, Bytes>>,
    dirty: std::sync::atomic::AtomicBool,
}

const SNAPSHOT_MAGIC: &[u8] = b"ENTMOOT-RET1\n";
const PERSIST_INTERVAL: Duration = Duration::from_secs(2);

impl RetainedStore {
    pub fn insert(&self, topic_name: String, payload: Bytes) {
        self.map.write().unwrap().insert(topic_name, payload);
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn remove(&self, topic_name: &str) {
        self.map.write().unwrap().remove(topic_name);
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
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
            let t = String::from_utf8(topic_bytes).map_err(|_| anyhow!("bad topic in snapshot"))?;
            self.map.write().unwrap().insert(t, Bytes::from(payload));
            count += 1;
        }
        Ok(count)
    }

    /// Atomically (write temp + rename) snapshot the store to disk.
    pub fn save_snapshot(&self, path: &std::path::Path) -> Result<()> {
        let mut out = Vec::with_capacity(4096);
        out.extend_from_slice(SNAPSHOT_MAGIC);
        for (t, payload) in self.snapshot() {
            out.extend_from_slice(&(t.len() as u32).to_be_bytes());
            out.extend_from_slice(t.as_bytes());
            out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            out.extend_from_slice(&payload);
        }
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &out)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Retained messages matching an MQTT subscription filter.
    pub fn matching(&self, filter: &str) -> Vec<(String, Bytes)> {
        self.map
            .read()
            .unwrap()
            .iter()
            .filter(|(t, _)| topic::topic_matches(filter, t))
            .map(|(t, p)| (t.clone(), p.clone()))
            .collect()
    }

    pub fn snapshot(&self) -> Vec<(String, Bytes)> {
        self.map.read().unwrap().iter().map(|(t, p)| (t.clone(), p.clone())).collect()
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
                        let payload = Bytes::from(sample.payload().to_bytes().into_owned());
                        debug!(topic = t, bytes = payload.len(), "retained stored");
                        store.insert(t.to_string(), payload);
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
                for (t, payload) in store.snapshot() {
                    let ke = topic::retained_keyexpr(&t, &scope);
                    if let Err(e) = query.reply(&ke, payload.to_vec()).await {
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
            store.insert(
                t.to_string(),
                Bytes::from(sample.payload().to_bytes().into_owned()),
            );
            fetched += 1;
        }
        if fetched > 0 {
            debug!(count = fetched, "retained catch-up complete");
        }
    }));

    Ok(tasks)
}
