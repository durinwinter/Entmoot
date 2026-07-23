//! Control-center-lite: mesh-wide force-disconnect (ENTERPRISE_ROADMAP.md,
//! "what's still missing: a way to *act* on \[the `$meta/clients`\] view").
//!
//! A client id lives on exactly one node's live connection at a time (a
//! same-id reconnect elsewhere already kicks the old one via the takeover
//! path in `session.rs`), but a control process watching `$meta/clients`
//! mesh-wide has no way to know *which* node that is without asking every
//! one of them. So disconnection is a broadcast query, not a directed one:
//! every node declares a queryable on the same `@ctl/disconnect` keyexpr
//! (mirroring the retained-store catch-up queryable in `retained.rs`); a
//! query carries the target client id as a Zenoh selector parameter
//! (`?client=<id>`), and only the node that actually holds a live
//! connection for that id kicks it and replies — the rest silently ignore a
//! query for a client they don't have. This rides the same Zenoh mesh as
//! `$meta/clients` itself: a control-center only needs a session already
//! peered into the mesh, not a new RPC layer.
//!
//! `@ctl` is `@`-prefixed like `@retained`/`@sys`/`@meta`, so client publish
//! topics can never collide with it (see `entmoot_core::topic`).

use anyhow::{anyhow, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use zenoh::Session;

const CTL_DISCONNECT_CHUNK: &str = "@ctl/disconnect";

/// How long [`disconnect_client`] waits for a reply before concluding no
/// node currently holds the client.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);

fn keyexpr(scope: &str) -> String {
    if scope.is_empty() {
        CTL_DISCONNECT_CHUNK.to_string()
    } else {
        format!("{scope}/{CTL_DISCONNECT_CHUNK}")
    }
}

/// Serve force-disconnect queries addressed to this node's mesh. Spawned
/// once at startup (see `lib.rs::run`); runs until the session closes.
pub async fn install(broker: Arc<crate::Broker>) -> Result<JoinHandle<()>> {
    let ke = keyexpr(&broker.cfg.scope);
    let queryable = broker
        .session
        .declare_queryable(&ke)
        .await
        .map_err(|e| anyhow!("ctl/disconnect queryable on {ke}: {e}"))?;
    let node_id = broker.cfg.id.clone();
    Ok(tokio::spawn(async move {
        while let Ok(query) = queryable.recv_async().await {
            let Some(client_id) = query.parameters().get("client") else {
                warn!("ctl/disconnect query missing 'client' parameter, ignoring");
                continue;
            };
            if broker.registry.kick(client_id) {
                info!(client = %client_id, node = %node_id, "client force-disconnected via control-center request");
                let reply_ke = query.key_expr().clone();
                if let Err(e) = query.reply(reply_ke, format!("kicked node={node_id}").into_bytes()).await {
                    warn!(client = %client_id, "ctl/disconnect reply failed: {e}");
                }
            }
            // A client this node doesn't hold gets no reply at all: the
            // requester only ever hears from the one node that actually
            // acted, same shape as "not found" needing no explicit signal.
        }
    }))
}

/// Outcome of a mesh-wide disconnect request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DisconnectOutcome {
    /// `node` is the id of the node that held the live connection and kicked it.
    Kicked { node: String },
    /// No node in the mesh currently holds a live connection for this client
    /// id (already offline, never connected, or the id was wrong).
    NotFound,
}

/// Ask the mesh to force-disconnect `client_id`, wherever its live
/// connection currently is. `session` just needs to be peered into the same
/// mesh (and scope) as the target — it does not need to be an Entmoot node
/// itself.
pub async fn disconnect_client(
    session: &Session,
    scope: &str,
    client_id: &str,
    timeout: Duration,
) -> Result<DisconnectOutcome> {
    let ke = keyexpr(scope);
    let selector = format!("{ke}?client={client_id}");
    let replies = session
        .get(&selector)
        .timeout(timeout)
        .await
        .map_err(|e| anyhow!("ctl/disconnect query on {selector}: {e}"))?;
    while let Ok(reply) = replies.recv_async().await {
        let Ok(sample) = reply.result() else { continue };
        let body = String::from_utf8_lossy(&sample.payload().to_bytes()).into_owned();
        let node = body.strip_prefix("kicked node=").unwrap_or(&body).to_string();
        return Ok(DisconnectOutcome::Kicked { node });
    }
    Ok(DisconnectOutcome::NotFound)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyexpr_scoping() {
        assert_eq!(keyexpr(""), "@ctl/disconnect");
        assert_eq!(keyexpr("plant-a"), "plant-a/@ctl/disconnect");
    }
}
