use anyhow::{Context, Result};
use clap::Parser;
use entmoot_core::NodeConfig;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Entmoot databus node — MQTT 3.1.1 frontend over the Entmoot bus.
///
/// Flags override the config file; security settings (users, ACLs, TLS) live
/// in the config file only. See config.example.toml.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// TOML config file (see config.example.toml)
    #[arg(long)]
    config: Option<PathBuf>,
    /// Print the SHA-256 hash of a password (for the config's users list) and exit
    #[arg(long, value_name = "PASSWORD")]
    hash_password: Option<String>,
    /// Control-center-lite utility mode: ask the mesh (via --config/--peer,
    /// same as a normal node) to force-disconnect this client id wherever
    /// its live connection currently is, print the outcome, and exit
    /// without starting a node. See ctl.rs.
    #[arg(long, value_name = "CLIENT_ID")]
    disconnect_client: Option<String>,
    /// Stable node identity (logs, metrics)
    #[arg(long)]
    id: Option<String>,
    /// MQTT listener address, e.g. 0.0.0.0:1883
    #[arg(long)]
    mqtt: Option<String>,
    /// Entmoot bus endpoint to listen on for peer links (repeatable)
    #[arg(long = "bus-listen", alias = "zenoh-listen", value_name = "BUS_LISTEN")]
    zenoh_listen: Vec<String>,
    /// Entmoot bus endpoint of a peer node, e.g. tcp/10.0.0.2:7447 (repeatable)
    #[arg(long = "peer")]
    peers: Vec<String>,
    /// StatefulSet bootstrap: the bus endpoint of the ordinal-0 pod, e.g.
    /// tcp/entmoot-0.entmoot-headless:7447. The same value is given to every
    /// pod in the StatefulSet; added to --peer on every pod except ordinal 0
    /// itself (detected by comparing against --id, so no shell/entrypoint
    /// script is needed in the container). Gossip completes the rest of the
    /// mesh from this one seed link — see k8s/README.md.
    #[arg(long)]
    peer_zero: Option<String>,
    /// Bus namespace prefix isolating the MQTT namespace on a shared fabric
    #[arg(long)]
    scope: Option<String>,
    /// Maximum accepted MQTT packet size in bytes
    #[arg(long)]
    max_packet_size: Option<usize>,
    /// Maximum rate (per second) of newly admitted CONNECTs; beyond this,
    /// CONNECT is refused with ServiceUnavailable instead of processed. 0 = unlimited
    #[arg(long)]
    connect_admission_rate: Option<u32>,
    /// Burst allowance for --connect-admission-rate
    #[arg(long)]
    connect_admission_burst: Option<u32>,
    /// Default staleness bound (seconds) for retained-message delivery; 0 = disabled.
    /// Per-namespace overrides are config-file only (see config.example.toml).
    #[arg(long)]
    retained_staleness_secs: Option<u64>,
    /// Cap Zenoh's own wire batch size (its MTU equivalent) in bytes; measure
    /// the real path MTU first with scripts/mtu-sweep.sh. Absent = Zenoh default.
    #[arg(long)]
    zenoh_link_mtu: Option<u16>,
    /// Quarantine a client id that reconnects more than this many times
    /// within --churn-window-secs. 0 = disabled. Schema rules for data
    /// validation are config-file only (see config.example.toml).
    #[arg(long)]
    churn_max_reconnects: Option<u32>,
    /// Rolling window (seconds) --churn-max-reconnects is measured over
    #[arg(long)]
    churn_window_secs: Option<u64>,
    /// How long (seconds) a flapping client is quarantined once caught
    #[arg(long)]
    churn_cooldown_secs: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if let Some(pw) = args.hash_password {
        println!("{}", entmoot_core::auth::sha256_hex(&pw));
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,zenoh=warn".into()),
        )
        .init();

    let mut cfg: NodeConfig = match &args.config {
        Some(path) => {
            let raw = std::fs::read_to_string(path)
                .with_context(|| format!("reading config {}", path.display()))?;
            toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?
        }
        None => NodeConfig::default(),
    };
    if let Some(id) = args.id {
        cfg.id = id;
    }
    if let Some(mqtt) = args.mqtt {
        cfg.mqtt_listen = mqtt;
    }
    if !args.zenoh_listen.is_empty() {
        cfg.zenoh_listen = args.zenoh_listen;
    }
    if !args.peers.is_empty() {
        cfg.peers = args.peers;
    }
    if let Some(peer_zero) = args.peer_zero {
        if !is_peer_zero_self(&peer_zero, &cfg.id) {
            cfg.peers.push(peer_zero);
        }
    }
    if let Some(scope) = args.scope {
        cfg.scope = scope;
    }
    if let Some(mps) = args.max_packet_size {
        cfg.max_packet_size = mps;
    }
    if let Some(rate) = args.connect_admission_rate {
        cfg.connect_admission_rate = rate;
    }
    if let Some(burst) = args.connect_admission_burst {
        cfg.connect_admission_burst = burst;
    }
    if let Some(secs) = args.retained_staleness_secs {
        cfg.retained_staleness_secs = secs;
    }
    if let Some(mtu) = args.zenoh_link_mtu {
        cfg.zenoh_link_mtu = Some(mtu);
    }
    if let Some(n) = args.churn_max_reconnects {
        cfg.churn_max_reconnects = n;
    }
    if let Some(secs) = args.churn_window_secs {
        cfg.churn_window_secs = secs;
    }
    if let Some(secs) = args.churn_cooldown_secs {
        cfg.churn_cooldown_secs = secs;
    }

    if let Some(client_id) = args.disconnect_client {
        match entmoot_node::query_disconnect(&cfg, &client_id).await? {
            entmoot_node::DisconnectOutcome::Kicked { node } => {
                println!("kicked: client {client_id:?} was connected to node {node:?}");
            }
            entmoot_node::DisconnectOutcome::NotFound => {
                println!("not found: no node in the mesh currently holds client {client_id:?}");
            }
        }
        return Ok(());
    }

    if cfg.auth.allow_anonymous && cfg.auth.users.is_empty() {
        tracing::warn!("running OPEN (anonymous, no ACLs enforced) — fine for dev, not for production");
    }

    let handle = entmoot_node::run(cfg).await?;

    #[cfg(unix)]
    if let Some(path) = args.config {
        tokio::spawn(reload_on_sighup(handle.broker.clone(), path));
    }

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    Ok(())
}

/// SIGHUP re-reads the config file and hot-reloads the safely-reloadable
/// parts (users, ACLs, schema rules, staleness bounds) via `Broker::reload`
/// — everything else (listeners, data_dir, TLS certs, ...) needs a restart.
/// A malformed file or an invalid schema logs and changes nothing; the node
/// keeps serving under its previous config. CLI-flag overrides applied at
/// startup are not re-applied on reload — a reload reflects the file as
/// written, so an active `--retained-staleness-secs` override (for example)
/// would revert to the file's value.
#[cfg(unix)]
async fn reload_on_sighup(broker: Arc<entmoot_node::Broker>, path: PathBuf) {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sighup = match signal(SignalKind::hangup()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("could not install SIGHUP handler: {e}");
            return;
        }
    };
    loop {
        sighup.recv().await;
        tracing::info!(file = %path.display(), "SIGHUP received, reloading config");
        match reload_from(&broker, &path) {
            Ok(()) => tracing::info!("config reload succeeded: users/ACLs/schema/staleness updated"),
            Err(e) => tracing::warn!("config reload failed, keeping previous config: {e:#}"),
        }
    }
}

#[cfg(unix)]
fn reload_from(broker: &entmoot_node::Broker, path: &Path) -> Result<()> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let new_cfg: NodeConfig = toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
    broker.reload(&new_cfg)
}

/// True if `peer_zero_endpoint` (e.g. "tcp/entmoot-0.entmoot-headless:7447")
/// names this very node's `id` — i.e. this node *is* the StatefulSet's
/// ordinal-0 pod and would otherwise try to peer with itself. Strips the
/// `scheme/` prefix, the `:port` suffix, and everything after the first
/// `.` (the rest of the DNS name), leaving just the hostname label to
/// compare against `id` — which in a StatefulSet deployment is the pod's
/// own stable name (`--id $(POD_NAME)`), the same string.
fn is_peer_zero_self(peer_zero_endpoint: &str, id: &str) -> bool {
    let without_scheme = peer_zero_endpoint.split_once('/').map_or(peer_zero_endpoint, |(_, rest)| rest);
    let host = without_scheme.split(':').next().unwrap_or(without_scheme);
    let first_label = host.split('.').next().unwrap_or(host);
    first_label == id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_zero_self_detection() {
        assert!(is_peer_zero_self("tcp/entmoot-0.entmoot-headless:7447", "entmoot-0"));
        assert!(is_peer_zero_self(
            "entmoot-0.entmoot-headless.default.svc.cluster.local:7447",
            "entmoot-0"
        ));
        assert!(!is_peer_zero_self("tcp/entmoot-0.entmoot-headless:7447", "entmoot-1"));
        // No accidental prefix match between "entmoot-0" and "entmoot-10".
        assert!(!is_peer_zero_self("tcp/entmoot-0.entmoot-headless:7447", "entmoot-10"));
    }
}
