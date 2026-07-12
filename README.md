# Entmoot

A distributed industrial MQTT databus: a mesh of small Rust nodes replaces a monolithic
broker (HiveMQ/EMQX). Each node speaks standard MQTT 3.1.1 to clients and uses the
Entmoot bus as the inter-node backbone — a publish on any node reaches subscribers on
every node, with no consensus cluster and no shared database. See [PLAN.md](PLAN.md)
for the architecture and phase roadmap, [ENTERPRISE_ROADMAP.md](ENTERPRISE_ROADMAP.md)
for the open-source enterprise feature track, and
[RESILIENCE_ROADMAP.md](RESILIENCE_ROADMAP.md) for reconnect-storm, partition,
and chaos-testing work.

## Build note (this machine)

`/home` is currently full, so point cargo's registry at the big drive before building:

```sh
export CARGO_HOME="/media/earthling/Caleb's Files3/.cargo-home"
```

## Quickstart

```sh
cargo test --workspace     # unit tests + 2-node cross-mesh integration test
./scripts/dev-mesh.sh      # 3-node local mesh: MQTT on 1883/1884/1885
```

Then in two other terminals:

```sh
cargo run -p entmoot-node --example sub -- --port 1883 --topic 'plant/#'
cargo run -p entmoot-node --example pub -- --port 1885 --topic plant/kiln1/temp --msg 993.5
```

The message enters node 3 and is delivered by node 1. Any MQTT 3.1.1 client
(mosquitto, rumqttc, a PLC gateway) works the same way.

## Canopy Console

The lightweight configuration console lives in [web/index.html](web/index.html).
It is static HTML/CSS/JS with no build step and exports TOML shaped for
`entmoot --config`. The console borrows the Fendtastic frontend's ent-shell
visual language using resized WebP assets under `web/assets/` to keep the first
load small. The Grove view sketches broker nodes, planned client groups, and
per-node client capacity so distributed deployments are easier to reason about
— today from the config being edited, not live telemetry.

A live visualizer built against this bus should key client liveness off
actual MQTT session activity, not tunnel/link state (a Nebula tunnel being up
says nothing about whether a given MQTT client is still connected). Nodes
publish connect/subscribe/unsubscribe/disconnect events on
`$meta/clients/<node-id>/<client-id>` for exactly this — but that's an
Entmoot fork behavior: a stock Zenoh peer has no MQTT client concept to
report on, so client-level fidelity in any dashboard requires pointing it at
this fork specifically, not a vanilla Zenoh deployment.

```sh
python3 -m http.server 4173 -d web
```

Then open <http://127.0.0.1:4173>.

## Node CLI

```sh
entmoot --id ent-1 \
        --mqtt 0.0.0.0:1883 \
        --bus-listen tcp/0.0.0.0:7447 \
        --peer tcp/10.0.0.2:7447 --peer tcp/10.0.0.3:7447 \
        --scope entmoot
```

`--scope` prefixes every mapped bus key, isolating the MQTT namespace when the nodes
share an Entmoot bus fabric with other systems. Multicast scouting is disabled by
design: nodes only join peers you name.

## Hardened mode

Security lives in the config file — see [config.example.toml](config.example.toml):

```sh
entmoot --hash-password 'the-password'   # -> sha256 for the users list
entmoot --config entmoot.toml            # users, ACLs, TLS, rate limits
```

With `allow_anonymous = false` + `default_policy = "deny"`, only listed users connect
and only granted topic filters can be published/subscribed. TLS runs on 8883 when a
`[tls]` section is present. Set `data_dir` to persist retained messages and offline
QoS 1 queues for persistent sessions. Set `health_listen` for Kubernetes
`/healthz` and `/readyz` probes.

## Status

Phase 1a+1b+1c: QoS 0/1 (QoS 2 accepted, relayed at-least-once), wildcards, Last Will,
keep-alive, retained messages mesh-wide (late-joining nodes catch up, persisted to
disk), password auth and mTLS client-cert identity, per-identity topic ACLs, MQTT
over TLS, publish rate limiting, connection caps, slow-consumer eviction,
persistent sessions with offline QoS 1 queueing, disk-backed queued backlog and
subscription metadata under `data_dir` (offline sessions rehydrate — subscriptions
and all — at startup, before the client reconnects, with ACLs re-checked against
current config), Prometheus `/metrics`, Kubernetes `/healthz` + `/readyz`, and
`$SYS/broker/<id>/…` node stats. Not yet: TLS cert rotation, MQTT 5 — remaining
Phase 1c items in [PLAN.md](PLAN.md).

Reconnect-storm protection: connect-admission control (`connect_admission_rate`)
sheds excess CONNECTs with a legible `ServiceUnavailable` ahead of
`max_connections`, and concurrent SUBSCRIBEs sharing a filter coalesce into
one retained-store scan instead of one per client. Partition staleness:
retained deliveries past `retained_staleness_secs` get a `$meta/<topic>`
companion flag instead of being presented as current. Fault injection for
both: [chaos/](chaos/) has a Toxiproxy-fronted mesh + partition/heal/storm
script runnable today, and Chaos Mesh manifests for once Phase 2 packaging
ships. `cargo run -p entmoot-node --example storm_bench` measures recovery
(live-traffic latency during a storm, time-to-rehydration, retained-scan
fan-out ratio) against any running node. `zenoh_link_mtu` caps Zenoh's wire
batch size below a link's real path MTU (`scripts/mtu-sweep.sh` finds it) so
fragmentation doesn't pollute those numbers. Client connect/subscribe/
disconnect events publish on `$meta/clients/<node-id>/<client-id>` for a
future live visualizer to key liveness off. See
[RESILIENCE_ROADMAP.md](RESILIENCE_ROADMAP.md) for the full six-workstream
plan — all six workstreams are done, with real gaps/tradeoffs found along
the way called out rather than glossed over.

Data governance: `[[schema]]` rules validate publishes on a matching topic
against a JSON Schema (drop or disconnect on failure), and
`churn_max_reconnects` quarantines a specific client id that reconnects too
often — Entmoot's take on HiveMQ's Data Hub schema/behavior policies. See
[ENTERPRISE_ROADMAP.md](ENTERPRISE_ROADMAP.md) for the full HiveMQ
feature-parity map and what's still open (Kubernetes packaging is the
biggest gap — this environment has no working Docker/kubectl to build and
verify it against, so it's flagged rather than shipped unverified).
