# Entmoot Resilience Roadmap

Entmoot's Phase 0-1 work made the mesh *correct*: retained state, persistent
sessions, ACLs, TLS, metrics. This roadmap is about what happens when a plant
network misbehaves — link flaps, a site partitions and heals, a few thousand
PLC gateways reconnect at once. Six workstreams, sequenced so the ones that
change the architecture land before the ones that just measure it:

```
1. reconnect storm / rehydration herd    ← done
2. partition merge semantics / staleness ← done
3. cluster-level fault injection         ← done
4. benchmarking methodology              ← done
5. Nebula/transport hygiene              ← done
6. visualizer honesty
```

## 1. Reconnect storm / rehydration herd — done

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

## 2. Partition merge semantics / staleness — done

Zenoh already resolves retained writes last-writer-wins on its own uHLC —
the merge primitive exists. The gap was legibility: a node that survived a
partition can hand a client a retained value that's correct-but-old without
saying so.

**Why not Zenoh's own sample timestamp:** the plan's first idea was to ride
Zenoh's per-sample uHLC timestamp end to end. That breaks down at exactly
the point that matters most — a late-joining node's catch-up `get()` against
the retained queryable. The public zenoh-1.9 API has no way to set a custom
`Timestamp` on `put()` or a queryable `reply()`; a reply always gets a fresh
timestamp at reply time, and `Sample`'s fields are crate-private with no
public constructor, so a `Sample` read from a live subscription can't be
stored and later re-served with its original timestamp intact either. Riding
the ambient Zenoh timestamp would silently make every value look "just
replied" fresh to a late-joiner, the opposite of the goal.

**What shipped instead:** the internal `[scope/]@retained/<topic>` payload
(already Entmoot-owned wire format, see `crates/entmoot-node/src/retained.rs`)
now carries its own 8-byte origin-write timestamp as a prefix
(`encode_envelope`/`decode_envelope`), captured with `SystemTime::now()` by
the node whose client actually published the retain. It travels unmodified
through live replication, queryable catch-up, and disk snapshot alike,
because Entmoot controls the encoding at every hop — unlike Zenoh's ambient
timestamp, which the reply path silently replaces. Documented tradeoff: this
is wall-clock, not a true HLC comparison, so it doesn't correct for clock
skew between nodes; it needs no new wire dependency and actually survives
the reply path, which the "correct" HLC answer didn't in this API version.

- `retained_staleness_secs` (node-wide default) and a `[[staleness]]`
  filter-matched override list (first match wins) define how old is too old,
  per namespace, exactly as sketched.
- A retained delivery past its bound gets a `$meta/<topic>` companion
  message — a new reserved topic space (`$meta`, mirroring `$SYS`: subscribe-
  only, unforgeable, invisible to bare `#`/`+` per MQTT-4.7.2-1) — routed
  through the same mesh-wide pub/sub path everything else uses, so it
  reaches every session subscribed to `$meta/#`, not just the one connection
  that triggered the check. Payload: `stale=true age_secs=<n> bound_secs=<m>`.
- Explicit invariant, now enforced in code rather than just documented: a
  retained value is only ever handed to a client silently as current when
  it's within its staleness bound; past it, the delivery still happens
  (MQTT-3.3.1-8 is unconditional) but is always paired with the flag.
- Tests in `crates/entmoot-node/tests/staleness.rs`: fresh delivery gets no
  flag, a delivery past the bound does, and a per-namespace override takes
  precedence over the node-wide default.

## 3. Cluster-level fault injection — done

Two layers, in `chaos/` (see `chaos/README.md` for the full writeup):

- **Toxiproxy, runnable today:** `chaos/toxiproxy-mesh.sh` fronts a real
  two-node Entmoot mesh's inter-node bus link with Toxiproxy, and
  `chaos/scenarios/partition-heal-reconnect-storm.sh` drives exactly the
  scenario turmoil was meant for in workstream 1 (partition N seconds, heal,
  storm the survivor with simultaneous reconnects) — at the real TCP layer
  instead of in-process, since that's where the constraint actually was.
  This is also the tool of record for Nebula-specific underlay paths once
  they exist, per the original plan, since Chaos Mesh only sees inside the
  cluster network.
- **Chaos Mesh, assumes Phase 2 packaging:** `chaos/k8s/*.yaml` —
  `NetworkChaos` site-partition, packet-loss, and latency/jitter manifests
  plus a recurring `Schedule` — against the StatefulSet Phase 2 packaging
  will produce (PLAN.md). Phase 2 hasn't shipped, so these are forward-
  looking and documented as such; selectors will need adjusting to whatever
  the real deployment's labels turn out to be.

## 4. Benchmarking methodology — done

`cargo run -p entmoot-node --example storm_bench` (see the doc comment at
the top of `crates/entmoot-node/examples/storm_bench.rs` for full usage)
measures recovery, not throughput, against any running node — local or a
real cluster:

- **Live-traffic latency during the storm**, via `hdrhistogram` with
  coordinated-omission correction (`record_correct`): steady-state
  publisher/subscriber pairs fire on a fixed schedule regardless of whether
  the previous round trip has completed, so a storm-induced stall shows up
  as the stall it is — backfilled across the gap — rather than a single
  outlier sample that percentiles quietly absorb.
- **Time-to-full-rehydration**: storm clients prime a persistent session,
  get torn down, then reconnect simultaneously (optionally around a real
  Toxiproxy partition/heal via `--toxiproxy-addr`/`--toxiproxy-proxy`,
  shelled out to `toxiproxy-cli toggle` rather than reimplementing its wire
  protocol), timed from reconnect to SUBACK/first message.
- **Fan-out ratio**: reads the target's `/metrics` for
  `entmoot_retained_scans_total` vs. `entmoot_subscribes_total` — the exact
  workstream-1 coalescing metric — plus `connect_shed_total` and
  `stale_retained_total`.

`emqtt-bench`/`criterion` from the original plan weren't used: emqtt-bench
measures throughput, not the recovery-shaped signals above, and criterion is
for micro-benchmarks, not a live multi-node storm — a purpose-built rumqttc
harness matches what's actually being measured, and reuses the same client
library already proven out in `tests/resilience.rs`.

**A real finding from running it, not a hypothetical:** the first version of
this harness had a bug — on a refused/dropped poll, `rumqttc`'s eventloop
retries immediately with no backoff, and the harness didn't handle that
case, so both the live client and the priming phase spun into a busy
reconnect loop that added tens of thousands of its own CONNECTs to the very
storm it was measuring (`connect_shed_total` read 84,629 for a 40-client
run before the fix, 75 after). Fixed by backing off on the live client and
giving up immediately on a shed priming attempt rather than hot-looping.
Running the harness against connect-admission control also surfaced a real
gap in workstream 1's design: admission control currently sheds *any*
CONNECT past its rate, live-traffic reconnects included — it has no way to
tell a rehydrating persistent session from an ordinary reconnect, so the
plan's stated policy ("shed rehydration before live traffic — rehydration
is retryable, live telemetry isn't") isn't fully realized. `clean_session`
isn't a reliable proxy (ordinary live producers use persistent sessions
too), so this needs a real signal — a client-supplied hint or a
priority scheme — before it can be fixed properly; tracked here rather than
patched with a heuristic that isn't actually justified.

Matrix (run manually today; a driver script that shells out to
`storm_bench` across the whole matrix is a natural follow-up, not yet
built): {partition 10s/60s/10min} × {1k/10k clients} × {single-site/two-site
over Nebula}.

## 5. Nebula/transport hygiene — done

Nebula itself isn't part of this codebase (it's an assumption about the
deployment this fork runs in, not something Entmoot integrates with
directly) — what shipped here is the codebase-side half: a real MTU clamp
and the double-encryption decision written down.

- **MTU clamp:** `zenoh_link_mtu` (config) / `--zenoh-link-mtu` (CLI) caps
  Zenoh's own wire batch size — its MTU equivalent
  (`transport/link/tx/batch_size` under the hood, verified against the real
  zenoh-1.9 config schema, not guessed) — below a link's real path MTU.
  Matters most for TCP bus links (what Entmoot actually uses today; there's
  no QUIC endpoint anywhere in this codebase yet): a QUIC-datagram link would
  auto-negotiate its own MTU from the underlying QUIC connection, but plain
  TCP has no such negotiation, so an oversized Zenoh batch over a
  small-MTU tunnel just gets silently IP-fragmented — and fragmented
  traffic pollutes every latency/throughput number until someone thinks to
  check for it. `scripts/mtu-sweep.sh` does the `ping -M do` binary search
  the plan called for to find the real number; `scripts/iperf3-fragmentation-check.sh`
  verifies full-size payloads survive it. A test
  (`crates/entmoot-node/tests/transport.rs`) proves a >65KB-analog payload
  (well over a clamped 1200-byte batch size) still crosses the mesh
  correctly — Zenoh's own fragmentation across multiple batches, not ours.
- **Double-encryption decision, written down:** if an Entmoot bus link runs
  entirely inside a Noise-secured overlay (Nebula or otherwise), plain `tcp/`
  Zenoh endpoints are the deliberate, correct choice — the overlay already
  provides confidentiality and peer identity, and a second TLS handshake on
  top adds CPU cost with no additional security in that specific topology.
  Zenoh does support `tls/` bus endpoints (PLAN.md), and that's the right
  choice for any bus link that leaves the overlay rather than staying inside
  it — same logic already applied to the MQTT client-facing port, which has
  supported TLS/mTLS since Phase 1a. The point of writing this down is that
  it should be a deliberate per-link decision an operator makes, not a
  default nobody thought about either way.

## 6. Visualizer honesty — not started

Key liveness off Zenoh session keepalives, not tunnel state. Emit this fork's
client connect/disconnect/subscription events onto `meta/clients/…` so the
visualizer consumes the same bus as everything else, and document that
client-level fidelity requires this fork (a stock Zenoh peer has no MQTT
client concept to report on).
