//! An Entmoot node: standard MQTT 3.1.1 on the front, Zenoh peer mesh on the back.
//!
//! Every accepted MQTT PUBLISH is `put` onto the Zenoh session; every MQTT
//! subscription becomes a Zenoh subscriber. Local delivery also round-trips
//! through Zenoh, so there is exactly one routing path and loops are
//! impossible by construction.

mod connection;
mod health;
mod metrics;
mod retained;
mod session;

pub use metrics::Metrics;
pub use retained::RetainedStore;
pub use session::SessionRegistry;

use anyhow::{anyhow, Context, Result};
use entmoot_core::auth::{Acl, Authenticator};
use entmoot_core::NodeConfig;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

pub struct Broker {
    pub session: zenoh::Session,
    pub cfg: NodeConfig,
    pub auth: Authenticator,
    pub acl: Acl,
    pub retained: Arc<RetainedStore>,
    pub registry: SessionRegistry,
    pub metrics: Metrics,
    conn_count: AtomicUsize,
}

impl Broker {
    pub fn connections(&self) -> usize {
        self.conn_count.load(Ordering::Relaxed)
    }
}

/// Handle to a running node; dropping it does not stop the node, call
/// [`BrokerHandle::shutdown`] (tests) or just let the process exit.
pub struct BrokerHandle {
    pub local_addr: std::net::SocketAddr,
    tasks: Vec<JoinHandle<()>>,
    pub broker: Arc<Broker>,
}

impl BrokerHandle {
    pub async fn shutdown(self) {
        for task in &self.tasks {
            task.abort();
        }
        if let Err(e) = self.broker.session.close().await {
            warn!("error closing zenoh session: {e}");
        }
    }
}

fn zenoh_config(cfg: &NodeConfig) -> Result<zenoh::Config> {
    let mut zc = zenoh::Config::default();
    let set = |zc: &mut zenoh::Config, key: &str, val: String| -> Result<()> {
        zc.insert_json5(key, &val)
            .map_err(|e| anyhow!("zenoh config {key}: {e}"))
    };
    set(&mut zc, "mode", r#""peer""#.into())?;
    // Hardened posture: never auto-join whatever else is on the LAN. Peers are
    // explicit (or, in Phase 2, injected from StatefulSet DNS). Gossip stays on
    // so peers-of-peers are learned across the explicit links.
    set(&mut zc, "scouting/multicast/enabled", "false".into())?;
    set(&mut zc, "listen/endpoints", to_json_array(&cfg.zenoh_listen))?;
    set(&mut zc, "connect/endpoints", to_json_array(&cfg.peers))?;
    Ok(zc)
}

fn to_json_array(items: &[String]) -> String {
    let quoted: Vec<String> = items.iter().map(|s| format!("{s:?}")).collect();
    format!("[{}]", quoted.join(","))
}

fn load_pem_certs(path: &str) -> Result<Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>> {
    rustls_pemfile::certs(&mut std::io::BufReader::new(
        std::fs::File::open(path).with_context(|| format!("opening {path}"))?,
    ))
    .collect::<std::result::Result<_, _>>()
    .with_context(|| format!("parsing certificates in {path}"))
}

fn tls_acceptor(cfg: &entmoot_core::config::TlsConfig) -> Result<TlsAcceptor> {
    use tokio_rustls::rustls;

    // Several rustls crypto providers end up linked (zenoh brings its own);
    // pin ours explicitly instead of relying on a process-global default.
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let certs = load_pem_certs(&cfg.cert_file)?;
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        std::fs::File::open(&cfg.key_file).with_context(|| format!("opening {}", cfg.key_file))?,
    ))
    .context("parsing TLS private key")?
    .ok_or_else(|| anyhow!("no private key found in {}", cfg.key_file))?;

    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .context("TLS protocol versions")?;

    let server_cfg = match &cfg.client_ca_file {
        // mTLS: clients must present a cert from this CA; its CN is their identity.
        Some(ca_path) => {
            let mut roots = rustls::RootCertStore::empty();
            for c in load_pem_certs(ca_path)? {
                roots.add(c).context("adding client CA certificate")?;
            }
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()
            .map_err(|e| anyhow!("building client-cert verifier: {e}"))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    }
    .with_single_cert(certs, key)
    .context("building TLS server config")?;
    Ok(TlsAcceptor::from(Arc::new(server_cfg)))
}

/// Common Name of the client certificate, if one was presented.
fn peer_cn(conn: &tokio_rustls::rustls::ServerConnection) -> Option<String> {
    let der = conn.peer_certificates()?.first()?;
    let (_, cert) = x509_parser::parse_x509_certificate(der.as_ref()).ok()?;
    let cn = cert.subject().iter_common_name().next()?;
    let cn = cn.as_str().ok()?.to_string();
    Some(cn)
}

/// Open the Zenoh session, bind the MQTT listener(s) and start accepting clients.
pub async fn run(cfg: NodeConfig) -> Result<BrokerHandle> {
    let session = zenoh::open(zenoh_config(&cfg)?)
        .await
        .map_err(|e| anyhow!("failed to open zenoh session: {e}"))?;
    info!(
        node = %cfg.id,
        zid = %session.zid(),
        listen = ?cfg.zenoh_listen,
        peers = ?cfg.peers,
        "zenoh peer session up"
    );

    let listener = TcpListener::bind(&cfg.mqtt_listen)
        .await
        .with_context(|| format!("binding MQTT listener on {}", cfg.mqtt_listen))?;
    let local_addr = listener.local_addr()?;
    info!(node = %cfg.id, addr = %local_addr, "MQTT listener up");

    let tls = match &cfg.tls {
        Some(tls_cfg) => {
            let acceptor = tls_acceptor(tls_cfg)?;
            let tls_listener = TcpListener::bind(&tls_cfg.listen)
                .await
                .with_context(|| format!("binding MQTT/TLS listener on {}", tls_cfg.listen))?;
            info!(
                node = %cfg.id,
                addr = %tls_listener.local_addr()?,
                mtls = tls_cfg.client_ca_file.is_some(),
                "MQTT/TLS listener up"
            );
            Some((tls_listener, acceptor))
        }
        None => None,
    };

    let metrics_listener = match &cfg.metrics_listen {
        Some(addr) => {
            let l = TcpListener::bind(addr)
                .await
                .with_context(|| format!("binding metrics listener on {addr}"))?;
            info!(node = %cfg.id, addr = %l.local_addr()?, "metrics endpoint up");
            Some(l)
        }
        None => None,
    };

    let health_listener = match &cfg.health_listen {
        Some(addr) => {
            let l = TcpListener::bind(addr)
                .await
                .with_context(|| format!("binding health listener on {addr}"))?;
            info!(node = %cfg.id, addr = %l.local_addr()?, "health endpoint up");
            Some(l)
        }
        None => None,
    };

    let state_dir = match &cfg.data_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir).with_context(|| format!("creating data_dir {dir}"))?;
            Some(std::path::PathBuf::from(dir))
        }
        None => None,
    };
    let retained_path = state_dir.as_ref().map(|dir| dir.join("retained.dat"));
    let queue_dir = state_dir.as_ref().map(|dir| dir.join("session-queues"));
    let retained = Arc::new(RetainedStore::default());
    let mut tasks =
        retained::wire(session.clone(), retained.clone(), cfg.scope.clone(), retained_path).await?;

    let broker = Arc::new(Broker {
        auth: Authenticator::new(&cfg.auth),
        acl: Acl::new(cfg.acl.clone(), cfg.auth.default_policy),
        retained,
        registry: SessionRegistry::new(
            cfg.max_queued_per_session,
            (cfg.slow_consumer_grace_ms > 0)
                .then(|| Duration::from_millis(cfg.slow_consumer_grace_ms)),
            queue_dir,
        ),
        metrics: Metrics::default(),
        session,
        cfg,
        conn_count: AtomicUsize::new(0),
    });

    if let Some(l) = metrics_listener {
        let b = broker.clone();
        tasks.push(tokio::spawn(metrics::serve(l, b)));
    }

    if let Some(l) = health_listener {
        let b = broker.clone();
        tasks.push(tokio::spawn(health::serve(l, b)));
    }

    if broker.cfg.sys_interval_secs > 0 {
        let b = broker.clone();
        let interval = Duration::from_secs(broker.cfg.sys_interval_secs);
        tasks.push(tokio::spawn(metrics::sys_publish(b, interval)));
    }

    if broker.cfg.session_expiry_secs > 0 {
        let b = broker.clone();
        let expiry = Duration::from_secs(broker.cfg.session_expiry_secs);
        tasks.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(SWEEP_INTERVAL);
            loop {
                tick.tick().await;
                b.registry.sweep(expiry);
            }
        }));
    }

    let b = broker.clone();
    tasks.push(tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    stream.set_nodelay(true).ok();
                    spawn_client(b.clone(), stream, peer, None);
                }
                Err(e) => {
                    error!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }));

    if let Some((tls_listener, acceptor)) = tls {
        let b = broker.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                match tls_listener.accept().await {
                    Ok((stream, peer)) => {
                        stream.set_nodelay(true).ok();
                        let acceptor = acceptor.clone();
                        let b = b.clone();
                        tokio::spawn(async move {
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    let cn = peer_cn(&tls_stream.get_ref().1);
                                    spawn_client(b, tls_stream, peer, cn);
                                }
                                Err(e) => info!(client = %peer, "TLS handshake failed: {e}"),
                            }
                        });
                    }
                    Err(e) => {
                        error!("TLS accept failed: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }));
    }

    Ok(BrokerHandle { local_addr, tasks, broker })
}

/// Decrements the connection count when a client task ends, however it ends.
struct ConnGuard(Arc<Broker>);
impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.conn_count.fetch_sub(1, Ordering::Relaxed);
    }
}

fn spawn_client<S>(
    broker: Arc<Broker>,
    stream: S,
    peer: std::net::SocketAddr,
    cert_identity: Option<String>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let count = broker.conn_count.fetch_add(1, Ordering::Relaxed) + 1;
    let guard = ConnGuard(broker.clone());
    if count > broker.cfg.max_connections {
        warn!(client = %peer, count, "connection limit reached, rejecting");
        drop(guard); // stream drops too: hard close before any MQTT handshake
        return;
    }
    tokio::spawn(async move {
        let _guard = guard;
        if let Err(e) = connection::serve(broker, stream, peer, cert_identity).await {
            info!(client = %peer, "connection ended: {e:#}");
        }
    });
}
