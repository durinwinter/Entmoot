//! Reconnect-storm benchmark harness (workstream 4 of RESILIENCE_ROADMAP.md).
//!
//! Measures recovery, not throughput: live-traffic latency *during* a storm
//! (HdrHistogram, coordinated-omission corrected — a generator that waits
//! for each response before sending the next would hide exactly the stalls
//! a storm causes), and per-client time-to-full-rehydration after a
//! simultaneous reconnect. Optionally reads the target node's `/metrics` to
//! report the retained-scan fan-out ratio the workstream-1 coalescing is
//! meant to shrink.
//!
//! Point it at any running entmoot node — a local `dev-mesh.sh` instance, or
//! a real cluster:
//!
//! ```sh
//! cargo run -p entmoot-node --example storm_bench -- \
//!     --port 1883 --metrics-port 9464 \
//!     --storm-clients 200 --live-clients 5 --duration-secs 20
//! ```
//!
//! To replay an actual partition/heal (see chaos/toxiproxy-mesh.sh), add
//! `--toxiproxy-addr 127.0.0.1:8474 --toxiproxy-proxy entmoot-a-link
//! --partition-secs 30` — the harness shells out to `toxiproxy-cli toggle`
//! at the right moments instead of re-implementing its wire protocol.

use clap::Parser;
use hdrhistogram::Histogram;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

#[derive(Parser, Debug)]
#[command(about = "Reconnect-storm benchmark: live-traffic latency + time-to-rehydration")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// Target node's MQTT port.
    #[arg(long, default_value_t = 1883)]
    port: u16,
    /// Target node's Prometheus /metrics port, if any (reports the
    /// retained-scan fan-out ratio and shed/stale counters when set).
    #[arg(long)]
    metrics_port: Option<u16>,
    /// Steady-state publisher/subscriber pairs measuring live-traffic latency
    /// throughout the whole run.
    #[arg(long, default_value_t = 5)]
    live_clients: u32,
    /// Live-traffic publish interval per client.
    #[arg(long, default_value_t = 100)]
    live_interval_ms: u64,
    /// Clients simulating the reconnect storm (persistent sessions primed,
    /// then reconnected simultaneously).
    #[arg(long, default_value_t = 200)]
    storm_clients: u32,
    /// Filter storm clients subscribe to.
    #[arg(long, default_value = "plant/#")]
    storm_topic: String,
    /// Total run duration.
    #[arg(long, default_value_t = 20)]
    duration_secs: u64,
    /// Toxiproxy API address (see chaos/toxiproxy-mesh.sh); enables an
    /// actual partition/heal around the storm instead of just a reconnect
    /// burst with no network fault.
    #[arg(long)]
    toxiproxy_addr: Option<String>,
    #[arg(long)]
    toxiproxy_proxy: Option<String>,
    /// How long to hold the partition before healing and storming.
    #[arg(long, default_value_t = 30)]
    partition_secs: u64,
}

fn now_nanos() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64
}

fn client_opts(host: &str, port: u16, id: &str) -> MqttOptions {
    let mut opts = MqttOptions::new(id, host, port);
    opts.set_keep_alive(Duration::from_secs(10));
    opts
}

/// Steady-state pub/sub pair: publishes its own send timestamp on a fixed
/// schedule and measures its own round trip, recording into `hist` with
/// coordinated-omission correction so a storm-induced stall shows up as the
/// stall it is rather than a single averaged-away outlier.
async fn run_live_client(
    host: String,
    port: u16,
    idx: u32,
    interval_ms: u64,
    hist: Arc<Mutex<Histogram<u64>>>,
    deadline: Instant,
) {
    let topic = format!("bench/live/{idx}");
    let (client, mut eventloop) =
        AsyncClient::new(client_opts(&host, port, &format!("bench-live-{idx}")), 16);
    if client.subscribe(&topic, QoS::AtMostOnce).await.is_err() {
        return;
    }
    let interval_us = interval_ms * 1_000;
    let mut tick = tokio::time::interval(Duration::from_millis(interval_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    loop {
        if Instant::now() >= deadline {
            return;
        }
        tokio::select! {
            _ = tick.tick() => {
                let payload = now_nanos().to_be_bytes().to_vec();
                let _ = client.publish(&topic, QoS::AtMostOnce, false, payload).await;
            }
            ev = eventloop.poll() => {
                match ev {
                    Ok(Event::Incoming(Packet::Publish(p))) if p.payload.len() >= 8 => {
                        let sent = u64::from_be_bytes(p.payload[..8].try_into().unwrap());
                        let latency_us = now_nanos().saturating_sub(sent) / 1_000;
                        let mut h = hist.lock().unwrap();
                        let _ = h.record_correct(latency_us.max(1), interval_us);
                    }
                    Ok(_) => {}
                    // rumqttc retries immediately on the next poll(); without a
                    // floor here a refused/dropped connection turns into a
                    // busy reconnect loop that itself pollutes the storm this
                    // client is meant to be measuring latency through.
                    Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                }
            }
        }
    }
}

enum StormOutcome {
    Rehydrated { millis: u64 },
    Shed,
    Failed,
}

/// Prime a persistent session (subscribe, then drop the connection without
/// destroying server-side state), so the later reconnect actually exercises
/// offline-session rehydration rather than a fresh subscribe.
async fn prime_storm_client(host: &str, port: u16, id: &str, topic: &str) {
    let mut opts = client_opts(host, port, id);
    opts.set_clean_session(false);
    let (client, mut eventloop) = AsyncClient::new(opts, 16);
    if client.subscribe(topic, QoS::AtLeastOnce).await.is_err() {
        return;
    }
    let _ = timeout(Duration::from_secs(5), async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::SubAck(_))) => return,
                Ok(_) => {}
                // Same reasoning as the live client: don't let a shed/refused
                // priming connect turn into a busy reconnect loop that adds
                // its own load to the very storm being measured.
                Err(_) => return,
            }
        }
    })
    .await;
    // Dropping client + eventloop here ends the TCP connection without a
    // clean_session=true DISCONNECT, leaving the persistent session intact.
}

/// Reconnect one storm client and time until its SUBACK (or backlog message,
/// whichever comes first) — the client-observed half of "time to full
/// rehydration". A `ServiceUnavailable` CONNACK counts as shed by admission
/// control, not a failure.
async fn reconnect_storm_client(host: String, port: u16, id: String, topic: String) -> StormOutcome {
    let start = Instant::now();
    let mut opts = client_opts(&host, port, &id);
    opts.set_clean_session(false);
    let (client, mut eventloop) = AsyncClient::new(opts, 16);
    let _ = client.subscribe(&topic, QoS::AtLeastOnce).await;
    let result = timeout(Duration::from_secs(15), async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::SubAck(_))) => return true,
                Ok(Event::Incoming(Packet::Publish(_))) => return true,
                Err(rumqttc::ConnectionError::ConnectionRefused(
                    rumqttc::ConnectReturnCode::ServiceUnavailable,
                )) => return false,
                Err(_) => return false,
                _ => {}
            }
        }
    })
    .await;
    match result {
        Ok(true) => StormOutcome::Rehydrated { millis: start.elapsed().as_millis() as u64 },
        Ok(false) => StormOutcome::Shed,
        Err(_) => StormOutcome::Failed,
    }
}

async fn fetch_metrics(addr: &str) -> Option<String> {
    let mut stream = tokio::net::TcpStream::connect(addr).await.ok()?;
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .ok()?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.ok()?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    Some(text.split_once("\r\n\r\n").map(|(_, b)| b.to_string()).unwrap_or(text))
}

fn metric_value(body: &str, name: &str) -> Option<f64> {
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(name)?;
        if !rest.starts_with('{') && !rest.starts_with(' ') {
            return None;
        }
        rest.rsplit(' ').next()?.parse().ok()
    })
}

fn toggle_toxiproxy(addr: &str, proxy: &str) {
    match Command::new("toxiproxy-cli").args(["-h", addr, "toggle", proxy]).status() {
        Ok(status) if status.success() => {}
        Ok(status) => eprintln!("toxiproxy-cli exited with {status}"),
        Err(e) => eprintln!("failed to run toxiproxy-cli: {e} (is it installed and on PATH?)"),
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let total_deadline = Instant::now() + Duration::from_secs(args.duration_secs);

    println!(
        "storm_bench: {} live client(s) @ {}ms, {} storm client(s) on {:?}, {}s run",
        args.live_clients, args.live_interval_ms, args.storm_clients, args.storm_topic, args.duration_secs
    );

    // 1. Steady-state live traffic runs for the whole test.
    let live_hist = Arc::new(Mutex::new(Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap()));
    let mut live_tasks = Vec::new();
    for i in 0..args.live_clients {
        live_tasks.push(tokio::spawn(run_live_client(
            args.host.clone(),
            args.port,
            i,
            args.live_interval_ms,
            live_hist.clone(),
            total_deadline,
        )));
    }

    // 2. Prime storm clients' persistent sessions.
    println!("priming {} persistent sessions...", args.storm_clients);
    let mut prime_tasks = Vec::new();
    for i in 0..args.storm_clients {
        let host = args.host.clone();
        let topic = args.storm_topic.clone();
        let id = format!("bench-storm-{i}");
        prime_tasks.push(tokio::spawn(async move {
            prime_storm_client(&host, args.port, &id, &topic).await;
        }));
    }
    for t in prime_tasks {
        let _ = t.await;
    }

    // 3. Partition/heal if a toxiproxy target was given; otherwise just
    // pause briefly so priming connections are fully torn down first.
    match (&args.toxiproxy_addr, &args.toxiproxy_proxy) {
        (Some(addr), Some(proxy)) => {
            println!("partitioning ({proxy} down) for {}s...", args.partition_secs);
            toggle_toxiproxy(addr, proxy);
            tokio::time::sleep(Duration::from_secs(args.partition_secs)).await;
            println!("healing ({proxy} back up)...");
            toggle_toxiproxy(addr, proxy);
        }
        _ => {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    // 4. Storm: reconnect everyone at once, timing rehydration.
    println!("firing the reconnect storm...");
    let mut storm_tasks = Vec::new();
    for i in 0..args.storm_clients {
        storm_tasks.push(tokio::spawn(reconnect_storm_client(
            args.host.clone(),
            args.port,
            format!("bench-storm-{i}"),
            args.storm_topic.clone(),
        )));
    }
    let mut rehydrate_hist = Histogram::<u64>::new_with_bounds(1, 300_000, 3).unwrap();
    let (mut rehydrated, mut shed, mut failed) = (0usize, 0usize, 0usize);
    for t in storm_tasks {
        match t.await {
            Ok(StormOutcome::Rehydrated { millis }) => {
                rehydrated += 1;
                let _ = rehydrate_hist.record(millis.max(1));
            }
            Ok(StormOutcome::Shed) => shed += 1,
            _ => failed += 1,
        }
    }

    // 5. Let live traffic run out the rest of the configured duration.
    let remaining = total_deadline.saturating_duration_since(Instant::now());
    if !remaining.is_zero() {
        tokio::time::sleep(remaining).await;
    }
    for t in live_tasks {
        t.abort();
    }

    // 6. Report.
    println!("\n=== live-traffic latency (ms), coordinated-omission corrected ===");
    {
        let h = live_hist.lock().unwrap();
        if h.is_empty() {
            println!("  no samples (check --host/--port and that the live topics aren't ACL-denied)");
        } else {
            let ms = |v: u64| v as f64 / 1000.0;
            println!("  count={}", h.len());
            println!("  p50={:.2}  p90={:.2}  p99={:.2}  p99.9={:.2}  max={:.2}",
                ms(h.value_at_percentile(50.0)),
                ms(h.value_at_percentile(90.0)),
                ms(h.value_at_percentile(99.0)),
                ms(h.value_at_percentile(99.9)),
                ms(h.max()),
            );
        }
    }

    println!("\n=== reconnect storm: time to full rehydration ===");
    println!("  admitted={rehydrated} shed={shed} failed={failed} (of {})", args.storm_clients);
    if !rehydrate_hist.is_empty() {
        println!("  p50={}ms  p90={}ms  p99={}ms  max={}ms",
            rehydrate_hist.value_at_percentile(50.0),
            rehydrate_hist.value_at_percentile(90.0),
            rehydrate_hist.value_at_percentile(99.0),
            rehydrate_hist.max(),
        );
    }

    if let Some(mp) = args.metrics_port {
        println!("\n=== node metrics ({}:{}) ===", args.host, mp);
        match fetch_metrics(&format!("{}:{mp}", args.host)).await {
            Some(body) => {
                let scans = metric_value(&body, "entmoot_retained_scans_total");
                let subs = metric_value(&body, "entmoot_subscribes_total");
                let shed_m = metric_value(&body, "entmoot_connect_shed_total");
                let stale = metric_value(&body, "entmoot_stale_retained_total");
                if let (Some(scans), Some(subs)) = (scans, subs) {
                    let ratio = if subs > 0.0 { scans / subs } else { 0.0 };
                    println!("  retained-scan fan-out ratio: {scans:.0} scans / {subs:.0} subscribes = {ratio:.3}");
                }
                if let Some(shed_m) = shed_m {
                    println!("  connect_shed_total: {shed_m:.0}");
                }
                if let Some(stale) = stale {
                    println!("  stale_retained_total: {stale:.0}");
                }
            }
            None => println!("  couldn't reach /metrics"),
        }
    }
}
