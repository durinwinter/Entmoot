use anyhow::{Context, Result};
use clap::Parser;
use entmoot_core::NodeConfig;
use std::path::PathBuf;

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

    if cfg.auth.allow_anonymous && cfg.auth.users.is_empty() {
        tracing::warn!("running OPEN (anonymous, no ACLs enforced) — fine for dev, not for production");
    }

    let _handle = entmoot_node::run(cfg).await?;
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    Ok(())
}
