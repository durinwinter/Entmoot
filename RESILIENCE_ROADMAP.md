# Entmoot Resilience Roadmap

Entmoot's Phase 0-1 work made the mesh *correct*: retained state, persistent
sessions, ACLs, TLS, metrics. This roadmap is about what happens when a plant
network misbehaves — link flaps, a site partitions and heals, a few thousand
PLC gateways reconnect at once. Six workstreams, sequenced so the ones that
change the architecture land before the ones that just measure it:

```
1. reconnect storm / rehydration herd   ← in progress
2. partition merge semantics / staleness
3. cluster-level fault injection
4. benchmarking methodology
5. Nebula/transport hygiene
6. visualizer honesty
```

## 1. Reconnect storm / rehydration herd — in progress

**Problem:** after a partition heals (or a node restarts), every client that
was attached to it reconnects at once. Each reconnect re-authenticates,
resumes a persistent session, and re-subscribes — and MQTT-3.3.1-8 requires
retained messages to follow every SUBACK. If many clients share overlapping
filters (`plant/#` is common in an industrial namespace), the node ends up
doing the same retained-match computation hundreds of times concurrently,
and — worse — a plain `max_connections` cap gives an overloaded node no way
to tell clients to back off; it just refuses the TCP accept, which looks
like an outage to the client and drives tighter reconnect loops.

**Design decisions for *this* codebase** (retained state is already fully
replicated in-memory on every node via `RetainedStore` — see
`crates/entmoot-node/src/retained.rs` — there is no remote "storage query"
to coalesce, unlike a client hitting a Zenoh storage plugin over the wire):

- **Admission control at CONNECT time.** A global GCRA rate limiter
  (`governor` crate) gates how fast new connections are *admitted* into the
  expensive path (auth, session attach, retained delivery), independent of
  the existing `max_connections` ceiling. When saturated, the node still
  completes the MQTT handshake far enough to reply
  `ConnAck(ServiceUnavailable)` before closing — a legible protocol-level
  signal instead of a bare TCP refusal, so well-behaved clients back off
  instead of hot-looping. Sheds before doing any auth/ACL/session work.
  Config: `connect_admission_rate` / `connect_admission_burst` (0 = off,
  default — no behavior change for existing deployments).
- **Coalescing retained-filter matching.** `RetainedStore::matching` was a
  linear scan under a `RwLock` on every SUBSCRIBE. Concurrent SUBSCRIBEs for
  the same filter (the exact reconnect-storm shape) now share one
  computation via a `moka::future::Cache<filter, Arc<Vec<(topic, payload)>>>`
  — moka's `get_with` blocks concurrent misses on the same key behind a
  single loader (stampede protection), invalidated wholesale on any retained
  mutation. This is the "singleflight" lever from the plan, applied to the
  computation that's actually redundant here (CPU + lock traffic, not a
  network round trip).
- **Deterministic testing.** `turmoil` was evaluated and parked: it requires
  the simulated process to run entirely on turmoil's own executor/socket
  shims, and Zenoh owns its own transport/runtime internals that aren't
  turmoil-aware, so a faithful "partition the mesh, heal it, storm the
  survivors" simulation isn't achievable without a much deeper fork of
  Zenoh's transport layer. Covered instead with real concurrent-load
  integration tests (many in-process rumqttc clients hammering a live node)
  that exercise the same code paths under real async concurrency, which is
  enough to prove the coalescing and admission-control behavior.

Still open in this workstream: LB/fork-level jittered backoff shaping is a
client/edge concern (the `backoff` crate covers the policy) — out of scope
for the node itself, which only owns the "reject legibly when saturated"
half.

## 2. Partition merge semantics / staleness — not started

Zenoh already timestamps every sample with a uHLC and resolves retained
writes last-writer-wins on that clock — the merge primitive exists. Planned:
attach the HLC timestamp to delivered retained messages (MQTT user property
once MQTT5 lands, or a parallel `meta/<topic>` topic in the meantime), define
a per-namespace staleness bound, and mark values older than that bound as
stale rather than presenting them as current. Write the heal-window behavior
down as an explicit invariant ("during heal, the overlay may serve values up
to partition-duration old, flagged") and test it before validating on a real
cluster.

## 3. Cluster-level fault injection — not started

Chaos Mesh (`NetworkChaos` CRDs) for in-cluster partition/loss/latency/
bandwidth scenarios, replaying the turmoil-style scripts we couldn't run at
the simulation layer. Toxiproxy or `tc netem` on the underlay for the
Nebula-specific paths (UDP-in-UDP, hole-punch recovery), since those tunnels
live below the Kubernetes cluster network Chaos Mesh operates in.

## 4. Benchmarking methodology — not started

Measure recovery, not throughput: time-to-full-rehydration, storage/queryable
fan-out ratio (now: the coalescing hit rate from workstream 1), and p99.9
live-traffic latency *during* a storm, using `hdrhistogram` with coordinated-
omission correction (a load generator that waits for each response hides
exactly the stalls a storm causes). Matrix: {partition 10s/60s/10min} ×
{1k/10k clients} × {single-site/two-site over Nebula}.

## 5. Nebula/transport hygiene — not started

Clamp MTU deliberately (path-MTU sweep inside the tunnel, Zenoh QUIC datagram
size below it, iperf3 full-size-payload verification) before any benchmark
number is trusted. Document the double-encryption call explicitly: intra-
Nebula links can run Zenoh plaintext (Noise already provides confidentiality
+ identity), TLS reserved for segments that leave the overlay.

## 6. Visualizer honesty — not started

Key liveness off Zenoh session keepalives, not tunnel state. Emit this fork's
client connect/disconnect/subscription events onto `meta/clients/…` so the
visualizer consumes the same bus as everything else, and document that
client-level fidelity requires this fork (a stock Zenoh peer has no MQTT
client concept to report on).
