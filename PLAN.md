# Entmoot — Distributed Industrial MQTT Databus

**Objective:** replace monolithic industrial MQTT brokers (HiveMQ, EMQX, VerneMQ) with a
horizontally scalable mesh of small Rust nodes. Each node speaks standard MQTT 3.1.1 to
clients (PLCs, gateways, SCADA, historians — unchanged) and uses an embedded
[Zenoh](https://zenoh.io) peer session as the inter-node backbone. The mesh *is* the broker.

```text
 PLC ──mqtt──▶ ┌──────────────┐          ┌──────────────┐ ◀──mqtt── SCADA
               │ entmoot-node │◀─zenoh──▶│ entmoot-node │
 Gateway ────▶ │  (peer A)    │          │  (peer B)    │ ◀──────── Historian
               └──────┬───────┘          └──────┬───────┘
                      └──────────zenoh──────────┘
                               (full peer mesh)
```

## Why this beats a monolithic broker

- **No consensus cluster.** HiveMQ-style clustering replicates session state via a
  distributed data grid — the operational pain point. Here, routing state is Zenoh's
  problem (gossip + peer mesh), and Phase 0 nodes are stateless: any node can die and
  clients just reconnect to another.
- **MQTT is the edge dialect, not the spine.** Inter-node traffic is Zenoh (zero-copy,
  wildcard-routed, transport-agnostic: TCP/TLS/QUIC/serial). MQTT topics map 1:1 onto
  Zenoh key expressions, so non-MQTT consumers (Twilight Bark agents, analytics, ROS2)
  can tap the same databus natively without an MQTT client.
- **Hardening is ours.** We own the frontend: per-identity ACLs, rate limits, packet-size
  caps, TLS/mTLS, and metrics live in our code path, not in a plugin sandbox.

## Compatibility with Twilight Bark

Entmoot nodes are ordinary Zenoh peers. Point them at the Twilight Bark bus (or run them
*as* part of it) and every MQTT publish becomes a Zenoh sample any Bark agent can
subscribe to. The `--scope` option prefixes all mapped key expressions
(e.g. `entmoot/plant/kiln1/temp`) so the MQTT namespace can't collide with Bark's
protobuf channels. Payloads are opaque bytes end-to-end.

## Topic ↔ key-expression mapping

| MQTT              | Zenoh keyexpr        | Note                          |
|-------------------|----------------------|-------------------------------|
| `plant/kiln1/temp`| `plant/kiln1/temp`   | verbatim (plus optional scope)|
| `plant/+/temp`    | `plant/*/temp`       | single-level wildcard         |
| `plant/#`         | `plant/**`           | multi-level wildcard          |
| `#`               | `**`                 |                               |

Restrictions (enforced, by design for an industrial namespace): no empty levels
(`a//b`), no leading/trailing `/`, and the verbatim characters `* $ ? #` are not allowed
inside topic names (they are Zenoh syntax). Clients violating this get SUBACK-failure /
disconnect per the MQTT spec.

---

## Phase 0 — Working mesh on bare processes  ← THIS PHASE (implemented)

Deliverables, all in this workspace:

- `crates/entmoot-core` — topic/keyexpr mapping + node config (unit-tested).
- `crates/entmoot-node` — the node binary (also a library so tests embed it):
  - MQTT 3.1.1: CONNECT/CONNACK, PUBLISH QoS 0/1 (QoS 2 accepted with full
    PUBREC/PUBREL/PUBCOMP handshake, relayed at-least-once), SUBSCRIBE/UNSUBSCRIBE,
    PING, DISCONNECT, keep-alive enforcement, Last Will & Testament.
  - Embedded Zenoh peer session; explicit peer list (multicast scouting **off** —
    hardened deployments don't auto-join strangers), gossip on.
  - All publishes go through Zenoh (even node-local delivery) — one code path, no
    routing loops by construction.
  - Config: CLI flags (`--id --mqtt --zenoh-listen --peer --scope --max-packet-size`).
- `scripts/dev-mesh.sh` — spin up a 3-node mesh locally.
- Integration test: 2 in-process nodes, rumqttc client subscribes via node A,
  publishes via node B, message crosses the mesh.
- Examples `sub.rs` / `pub.rs` for manual poking without mosquitto installed.

**Deliberately deferred from Phase 0:** retained messages, persistent (cleanSession=0)
sessions, auth. Documented, not forgotten — see Phase 1.

## Phase 1 — MQTT completeness + hardening  (1a + 1b implemented)

Done (Phase 1a):

- ✅ Retained messages, mesh-wide: retained publishes are mirrored into an internal
  `[scope/]@retained/…` keyspace ('@' chunks are zenoh-verbatim and rejected in client
  topics, so the space is unforgeable). Every node replicates it via a subscriber,
  serves it via a queryable, and a late-joining node catches up with one `get`.
  Empty-payload clears propagate. In-memory today; RocksDB persistence later.
- ✅ AuthN: users + SHA-256 password hashes in the TOML config
  (`entmoot --hash-password` generates them); `allow_anonymous` switch.
- ✅ AuthZ: per-identity topic ACLs with `default_policy = "deny"` mode. Denied
  subscription → SUBACK failure; denied publish → dropped-and-logged (acked, so a
  misconfigured PLC doesn't retry-storm); wills are ACL-checked too. Subscription
  grants use conservative filter-coverage (`plant/#` covers `plant/+/temp`).
- ✅ MQTT over TLS (8883) via rustls (provider pinned; zenoh links can use `tls/`
  endpoints via zenoh config).
- ✅ Overload protection: per-connection publish rate limit (token bucket,
  violators disconnected) and a max-connections cap.
- ✅ TOML config file (`config.example.toml`) with CLI overrides.

Done (Phase 1b):

- ✅ Persistent sessions (cleanSession=0): subscriptions and QoS 1 queueing
  survive disconnects (bounded queue, drop-oldest), session takeover per
  MQTT-3.1.4-2, configurable offline expiry sweep.
- ✅ mTLS client certs: with a `client_ca_file` configured, TLS clients must
  present a cert from that CA and its CN becomes the ACL identity.
- ✅ Retained persistence to disk (`data_dir`, debounced snapshot — survives
  whole-mesh restart).
- ✅ Prometheus `/metrics` endpoint (`metrics_listen`).
- ✅ Slow-consumer eviction: a client whose outbound queue stays full past
  `slow_consumer_grace_ms` is disconnected (will fires; persistent sessions
  fall back to offline queueing) instead of stalling mesh subscriber tasks.
- ✅ `$SYS` topics: per-node stats on `$SYS/broker/<id>/…` every
  `sys_interval_secs`, mesh-wide. Mapped onto the verbatim `@sys` keyspace, so
  they are subscribe-only, unforgeable, and invisible to `#`/`+`
  (MQTT-4.7.2-1 for free).
- ✅ Disk-backed offline QoS 1 backlog for persistent sessions when `data_dir`
  is configured. Already-queued messages survive a node restart and drain when
  the client reconnects.
- ✅ Kubernetes-style `/healthz` and `/readyz` endpoints via `health_listen`.

Done (Phase 1c):

- ✅ Persisted subscription metadata and restart-time offline session
  rehydration: subscribed filters (with granted QoS and owning identity) are
  written under `data_dir` alongside the offline queue. At startup the node
  replays them, re-declaring the mesh subscriptions and re-checking each
  filter against the *current* ACL, before any client reconnects — an offline
  persistent session resumes collecting (and queueing) messages immediately
  after a restart instead of only once the device itself reconnects. A grant
  removed since the last run is dropped and logged, not silently reinstated.

Remaining (Phase 1c):

- TLS cert rotation / reload without restart.
- MQTT 5 (via `mqttbytes::v5`): session expiry, shared subscriptions, reason codes.

### Reconnect-storm protection

Phase 1's persistent-session rehydration means a partition heal or node
restart brings every affected client back at once, all resuming sessions and
re-subscribing (with MQTT-3.3.1-8 retained delivery on every SUBACK). Two
defenses, detailed in [RESILIENCE_ROADMAP.md](RESILIENCE_ROADMAP.md):

- ✅ Connect-admission control: a GCRA rate limiter (`connect_admission_rate`
  / `connect_admission_burst`) gates how fast new CONNECTs enter auth/session
  work, refusing the excess with `ServiceUnavailable` — a legible signal
  instead of a bare TCP refusal — ahead of the existing `max_connections`
  cap. Off by default (rate 0).
- ✅ Retained-match coalescing: concurrent SUBSCRIBEs sharing a filter (the
  reconnect-storm shape) share one retained-store scan via a `moka` cache
  with singleflight semantics, instead of each client re-scanning
  independently. `entmoot_retained_scans_total` vs. `entmoot_subscribes_total`
  in `/metrics` is the fan-out ratio this collapses.

### Partition staleness

A value that survived a partition is correct-but-old, not current. Every
retained entry now carries its origin write time; `retained_staleness_secs`
(plus per-filter `[[staleness]]` overrides) defines how old is too old, and a
delivery past that bound gets a `$meta/<topic>` companion message
(`stale=true age_secs=<n> bound_secs=<m>`) alongside the normal retained
PUBLISH, delivered to anyone subscribed to `$meta/#` — a new reserved topic
space mirroring `$SYS` (unforgeable, invisible to bare wildcards per
MQTT-4.7.2-1). Off by default (`retained_staleness_secs = 0`). See
[RESILIENCE_ROADMAP.md](RESILIENCE_ROADMAP.md) for why this rides in the
payload rather than Zenoh's own sample timestamp.

### Why not OpenZiti for this?

Considered and parked: OpenZiti is a zero-trust *connectivity* overlay (identity-based
dial/bind, no listening ports on the underlay) — it has no pub/sub, topics, fan-out, or
retained state, so it addresses "who may reach the databus", not "how messages route".
It cannot replace zenoh here, and app-embedding it is unattractive (immature Rust SDK,
C bindings). It *can* complement Entmoot as an optional Phase 2 deployment profile:
ziti tunneler/router sidecars carrying the 1883/8883 client links and inter-site peer
links, at the cost of operating a ziti controller + edge routers. Native TLS/mTLS in
our own code path covers the core need with less operational surface, so that ships
first.

## Phase 2 — Kubernetes packaging

- Static musl build → distroless image (~15 MB).
- StatefulSet + headless Service: peers discover each other via stable DNS
  (`entmoot-0.entmoot-hl`, …) — no discovery service needed.
- LoadBalancer/NodePort for 1883/8883 client ingress; readiness gate = Zenoh session up.
- PodDisruptionBudget, NetworkPolicy (only 1883/8883 in, 7447 peer-to-peer).
- Local dev loop with kind + Tilt, manifests shared with prod via Kustomize overlays
  (deferred per earlier discussion until Docker is available on a dev box).

## Phase 3 — Industrial differentiators

- Sparkplug B awareness (birth/death certificates mapped to LWT + retained state).
- Bridge profile into Twilight Bark scopes; protobuf-tagged sample encodings.
- Zenoh storage plugins for history/replay (historian-lite).
- Chaos suite: kill-a-node-under-load failover tests, reconnect-storm shaping.

---

## Running Phase 0

```sh
cargo test --workspace          # unit + cross-node integration test
./scripts/dev-mesh.sh           # 3-node local mesh on 1883/1884/1885
cargo run -p entmoot-node --example sub -- --port 1883 --topic 'plant/#'
cargo run -p entmoot-node --example pub -- --port 1885 --topic plant/kiln1/temp --msg 993.5
```
