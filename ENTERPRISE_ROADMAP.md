# Entmoot Enterprise Roadmap

Entmoot is an open-source industrial MQTT databus for plant operators who need
broker-grade reliability without adopting a monolithic broker cluster. MQTT
stays the edge protocol for PLC gateways, SCADA, historians, and existing
clients. Zenoh is non-negotiable as the distributed spine: routing, peer mesh,
and non-MQTT databus access are core to the design.

The goal is enterprise-grade feature parity with VerneMQ, HiveMQ, and EMQX
where plant operations actually need it, while keeping the operating model
small enough for on-prem Kubernetes clusters from vendors such as Oxide,
Red Hat, and similar industrial platforms.

## Product Principles

- Open source first. No open-core split, license checks, or paywalled broker
  features.
- Industrial operations first. Favor predictable behavior, explicit topology,
  and recoverability over cloud-only convenience.
- Zenoh as the spine. Entmoot should explain and expose its Zenoh topology
  instead of hiding it behind broker-cluster language.
- Kubernetes-native, on-prem friendly. StatefulSet operation, persistent
  volumes, readiness gates, metrics, and controlled node drain are first-class.
- MQTT compatibility before novelty. MQTT 3.1.1 should be boringly solid;
  MQTT 5 comes after enterprise operational parity is credible.

## Enterprise Parity Targets

### Durability and Recovery

- Disk-backed retained messages.
- Disk-backed offline persistent-session queues.
- Persisted session subscriptions and queue metadata.
- Restart recovery that can rehydrate offline sessions and resume queueing.
- Explicit partition/restart/failover semantics in documentation.
- Backup and restore procedures for node-local state.

### Operations Plane

- `/healthz` liveness endpoint.
- `/readyz` readiness endpoint for Kubernetes traffic gates.
- Prometheus metrics with stable names and label discipline.
- Structured logs suitable for Loki, OpenSearch, or Splunk.
- Graceful drain mode: stop accepting new clients, let existing clients leave,
  and publish node status.
- Hot reload for config that can safely change at runtime.
- TLS and mTLS certificate reload without process restart.

### Security and Governance

- Password hashing suitable for human-managed credentials.
- mTLS identity mapping beyond Common Name where deployments need SANs or SPIFFE.
- Dynamic users and ACLs from file, Kubernetes Secret, or external controller.
- Audit events for connection, auth failure, ACL denial, config reload, and
  operator actions.
- Tenant/project scopes with quotas for connections, publish rate, retained
  count, and offline queue depth.

### Protocol Compatibility

- MQTT 3.1.1 conformance hardening and compatibility matrix.
- QoS 1 redelivery tracking for outbound messages.
- MQTT 5 after parity foundations: reason codes, session expiry, user
  properties, shared subscriptions, response topics, and server keep-alive.
- Sparkplug B awareness as an industrial differentiator.

### Kubernetes Packaging

- Distroless container image.
- StatefulSet with persistent volume claims.
- Headless Service for stable peer DNS.
- Service and NetworkPolicy examples for MQTT, MQTT/TLS, Zenoh, and ops ports.
- Helm or Kustomize overlays for development, staging, and production.
- Kind-based local test environment.
- PodDisruptionBudget and graceful rolling restart story.

### Benchmarks and Proof

- Client compatibility matrix across common MQTT clients and PLC gateways.
- Throughput, latency, retained fan-out, reconnect storm, and offline queue
  benchmarks.
- Chaos tests: kill nodes under load, restart with offline sessions, partition
  the Zenoh mesh, and recover.
- Side-by-side operational comparison with VerneMQ, HiveMQ, EMQX, and Mosquitto.

## First Sprint

The first sprint makes Entmoot more credible as a Kubernetes-operated plant
broker without changing its public MQTT surface.

1. Add a durable offline queue store under `data_dir`.
2. Add Kubernetes health and readiness endpoints.
3. Document the durability semantics honestly.
4. Keep the implementation simple and auditable before optimizing write
   batching or storage engines.

## Next Sprint Candidates

- ✅ Persist subscription metadata so offline sessions can be fully rehydrated
  after process restart. Done: see Phase 1c in [PLAN.md](PLAN.md).
- Add a drain endpoint or signal-driven drain mode for rolling upgrades.
- Add Kubernetes manifests for StatefulSet, headless peer service, and PVCs.
- Add a Kind smoke test that starts a three-node Entmoot mesh.
- Add compatibility tests for MQTT 3.1.1 clients used in industrial settings.

## HiveMQ feature-parity map

HiveMQ's Enterprise Suite (Enterprise Security Extension, Data Governance
Hub, Enterprise Bridge/Kafka extensions, Control Center, Kubernetes
Operator) is the concrete bar for "enterprise-grade" in this space. Below is
what each area actually does in HiveMQ, Entmoot's current status against it,
and — the point of this section — whether closing the gap fits the
no-consensus, no-shared-database architecture as-is, needs a decentralized
reframing, or is a genuinely hard open problem in that architecture. Sourced
against HiveMQ's current docs, not assumed from memory; see Sources at the
end of the response this section was written from.

### Clustering, session HA, and shared subscriptions — the central tension

HiveMQ's cluster is masterless in the sense of no fixed coordinator, but it
still replicates every piece of persistent state (sessions, queued
messages, retained messages) to a configurable number of follower nodes
(replica-count, default 2) via an internal leader/follower protocol per
item — a distributed data grid, exactly the operational model PLAN.md
already rejects ("the operational pain point"). That rejection is *right*,
but it has a real cost we should name plainly instead of glossing over:

- **Retained messages**: full parity today. Every node holds a complete
  replica (`RetainedStore`), kept current via Zenoh pub/sub + queryable
  catch-up. No gap.
- **Persistent sessions (subscriptions + offline QoS 1 queue)**: **not** at
  parity, and this is the one real architectural gap in this whole map.
  Today a session lives on exactly the node that created it — in-memory
  plus that node's own disk (`data_dir/session-queues/`). A Kubernetes
  StatefulSet pod restart survives fine (same PVC remounts), but losing that
  specific node/volume for good loses the session's queued backlog and
  subscription list, with no other node able to take over — a real single
  point of failure HiveMQ's replication is specifically built to eliminate.
  **Decentralized-compatible fix, not yet built:** apply the exact pattern
  `RetainedStore` already proves out — replicate session subscriptions and
  a bounded offline queue into an internal `@session/<client-id>/…` Zenoh
  keyspace (put/subscribe/queryable-catchup, same three-task shape as
  `retained.rs`), so any node can rehydrate any client's session on
  reconnect, not just its original owner. The one piece that needs new
  design (not just copy-pasting the retained pattern) is avoiding two nodes
  both believing they own a client's *live* connection at once — today
  that's solved locally via `SessionState`'s epoch/takeover mechanism;
  extending it cross-node needs a lightweight liveliness check (a
  Zenoh liveliness token per active session, queried before a node
  attaches a session) rather than a new consensus system. Worth scoping as
  its own project, not a quick add.
- **Shared subscriptions** (MQTT 5, HiveMQ-pioneered): not implemented at
  all — Entmoot has no MQTT 5, and 3.1.1 has no shared-subscription concept
  either. Load-balancing shared subscribers *within one node* is easy
  (ordinary round-robin over local sinks). Load-balancing them *across
  nodes* without duplicate delivery is a genuinely hard problem for a
  stateless-node mesh — it needs exactly the kind of cross-node
  coordination Entmoot's whole design avoids. Flagging this honestly as an
  open design question, not a checkbox, is more useful than a plan that
  quietly assumes it away.

### Data Governance Hub (schema validation + behavior policies) — done (v1)

HiveMQ validates payloads against JSON Schema/Protobuf per topic filter
(pass/drop/reroute), and models client lifecycle as a state machine to
catch misbehaving clients (behavior policies) — a generalized, declarative
version of what Entmoot currently does as one-off hardcoded checks (ACLs,
publish rate limiting, connect admission, slow-consumer eviction). This was
the **most decentralization-friendly item on this whole list**: schema and
behavior validation are inherently per-message, per-connection, per-node
decisions — no cross-node coordination needed at all, unlike session HA
above.

- **Schema (data) policy**: `[[schema]]` rules (`entmoot_core::schema`),
  shaped exactly like `AclRule`/`StalenessRule` — topic-filter matched, first
  match wins. A publish on a matching topic must parse as JSON and validate
  against the configured JSON Schema (the `jsonschema` crate, no
  remote-`$ref` resolution — schemas are self-contained, compiled at
  startup so a malformed one is a startup error, not a silent no-op under
  load), or `on_fail` applies: `drop` (acked, not delivered — same
  reasoning as an ACL-denied publish, v3.1.1 has no error ack) or
  `disconnect`. Protobuf schemas are still a bigger lift and not built.
  `entmoot_schema_denied_total` in `/metrics`. Tests in
  `crates/entmoot-node/tests/schema.rs`.
- **Behavior policy**: rather than a generic state-machine framework (a much
  bigger project), built the one concrete case HiveMQ's own behavior-policy
  marketing example leads with and Entmoot didn't have yet — reconnect
  churn. `churn_max_reconnects`/`churn_window_secs`/`churn_cooldown_secs`
  quarantine a *specific client id* that reconnects too often within a
  window, refusing its CONNECT with `ServiceUnavailable` for a cooldown.
  This is deliberately the identity-aware complement to workstream 1's
  connect-admission control, which sheds an aggregate storm without caring
  who anyone is — churn quarantine catches one client flapping even while
  the rest of the mesh is quiet. `entmoot_churn_quarantined_total` in
  `/metrics`. Tests in `crates/entmoot-node/src/churn.rs` (unit) and
  `crates/entmoot-node/tests/churn.rs` (integration).

A full HiveMQ-style declarative behavior-policy *engine* (arbitrary client
state machines, not just the one built-in churn case) remains a bigger,
separate project if more cases accumulate — this shipped the schema half in
full and the one behavior case that mattered most given this repo's own
reconnect-storm work, not a generalized framework nobody's asked to extend
yet.

### Enterprise Security Extension (LDAP, OAuth2/JWT, RBAC, dynamic permissions) — partly done

Entmoot auth used to be a static TOML file only: SHA-256 password hashes,
static per-user ACL rules, mTLS CN identity — no LDAP/AD, no OAuth2/JWT, no
runtime reload, no per-client dynamic permission templating, no certificate
revocation checking (CRL/OCSP). None of this needs cross-node
coordination — auth and ACL decisions are already, and always will be, a
stateless per-connection lookup evaluated locally by whichever node the
client happens to land on. The gap was entirely about *what the lookup
source is*, not the architecture. Planned order was: (1) hot-reload, (2)
JWT/OAuth2 bearer validation, (3) LDAP/AD bind, (4) per-client dynamic
permission placeholders, (5) CRL/OCSP checking.

(1) and (2) are done:

- **Hot reload** (`Broker::reload`, wired to SIGHUP in `main.rs`): users,
  `[[acl]]`, `[[schema]]`, and staleness settings swap in atomically —
  built and validated before anything is replaced, so a malformed file or a
  bad schema is logged and changes nothing rather than half-applying. The
  swap itself uses `ArcSwap` (lock-free reads on every publish/subscribe/
  connect, which is the hot path this touches); listeners, `data_dir`, and
  TLS certs still need a restart. Verified two ways: `Broker::reload`
  called directly in integration tests (three tests in
  `tests/hot_reload.rs` — new user, ACL change, and a rejected bad reload,
  all without restarting the node), and a manual end-to-end smoke test
  sending a real `SIGHUP` to a running process and confirming a newly added
  user could connect. The SIGHUP plumbing itself isn't covered by the
  automated suite: `cargo test` runs many tests in one process, and a real
  signal would hit every test's handler at once, not just the one under
  test — noted in the test file rather than glossed over.
- **JWT bearer auth**: `[auth.jwt]` — HS256 with a static shared secret, not
  full OAuth2/OIDC with JWKS discovery (a live-key-rotation flow is a
  separate, bigger feature; static-key verification covers the common
  on-prem/industrial case of a fixed signing key or a locally mirrored
  key). Additive to local users: a CONNECT whose username isn't a known
  local user gets its password tried as a JWT instead of an outright
  refusal; a username that *is* a known local user must still authenticate
  with that user's own password, never a token. `identity_claim`'s value
  (default `sub`) becomes the authenticated identity for ACL matching.
  Tests: unit tests in `crates/entmoot-core/src/auth.rs` (valid token,
  wrong secret, wrong issuer, expired, missing `exp`, and the
  known-user-can't-use-a-token case) plus an integration test in
  `tests/jwt_auth.rs` driving real CONNECTs with real signed tokens.

(3), (4), (5) remain open.

### Control Center (live admin UI + REST API)

HiveMQ's Control Center shows connected clients live and can force-
disconnect one via the UI/REST API. Entmoot's Canopy Console
(`web/index.html`) is still a static config-authoring tool only — no live
UI has been built — but both backend primitives a live one needs now exist.
Workstream 6 of [RESILIENCE_ROADMAP.md](RESILIENCE_ROADMAP.md) added
`$meta/clients/<node-id>/<client-id>` connect/subscribe/unsubscribe/
disconnect events, mesh-wide, the same way `$SYS` already works — a process
that subscribes to `$meta/clients/#` mesh-wide already has a live view of
every client on every node for free, no cluster-aware RPC layer needed.
**The other half — a way to *act* on that view — has now landed too**
(`crates/entmoot-node/src/ctl.rs`): force-disconnect is a broadcast Zenoh
query on an internal `@ctl/disconnect` keyspace (`?client=<id>`), and
whichever node currently holds that client's live connection kicks it and
replies; every other node silently ignores a query for a client it doesn't
have. Reachable either as a library call
(`entmoot_node::ctl::disconnect_client`, for any process already peered
into the mesh) or via `entmoot --disconnect-client <id>` (a one-shot CLI
utility mode that opens a throwaway session, queries, prints the outcome,
and exits — see `crates/entmoot-node/tests/control_center.rs` for the
mesh-wide integration test). Auth failures and ACL denials (publish,
subscribe, will) are now also published onto the same `$meta/clients` bus
alongside the existing `tracing::warn!` logs, closing out the audit-event
half of priority item 4 too. What's still missing for a real Control
Center: the actual live UI/dashboard consuming both of these — a
`$meta/clients` subscriber feeding a client table, wired to a
force-disconnect button. That's a frontend task now, not a protocol design
one.

### Enterprise Bridge Extension / Kafka Extension (federation)

HiveMQ bridges separate broker clusters (or Kafka) with bidirectional,
topic-mapped forwarding. PLAN.md Phase 3 already lists "bridge profile into
Twilight Bark scopes" — the same idea, narrowly scoped. Not built. Worth
noting explicitly: Entmoot's architecture makes broad "federate two
Entmoot meshes" bridging mostly unnecessary — two Entmoot deployments can
just peer their Zenoh sessions directly and become one mesh, no MQTT-level
bridge required, unlike HiveMQ where each cluster is a hard boundary. A
bridge earns its keep specifically for (a) crossing a real protocol
boundary (Kafka, or a third-party broker Entmoot doesn't control — a
customer's existing Mosquitto/EMQX), or (b) crossing a deliberate
trust/security boundary where full Zenoh peering is undesirable (a DMZ, a
partner network). Scope any bridge work narrowly to those two cases rather
than a generic "cluster federation" feature the architecture doesn't need.

### Kubernetes Operator / Helm / packaging

**Phase 2 packaging has landed** (`Dockerfile`, `k8s/` — see `k8s/README.md`
for the full quickstart): a distroless image from a static musl build, a
StatefulSet + headless Service using stable pod DNS for peer bootstrap (no
external discovery service needed), a client-facing Service,
PodDisruptionBudget, NetworkPolicy (1883/8883 open, 7447 peer-to-peer only),
and dev/staging/production Kustomize overlays. The Rust side (the
`--peer-zero` bootstrap flag and its self-detection logic) is unit-tested
and passes in this environment; the manifests were rendered and their
content inspected field-by-field with `kustomize build`, but — worth
repeating because it matters for anyone relying on this — **none of it has
been applied to a real cluster or had its Docker image actually built**,
since this environment has neither a Docker daemon nor `kubectl`/`kind`.
`k8s/README.md` is written for a human to run the missing verification step
(build the image, `kind load`, `kubectl apply -k`, confirm the mesh forms)
on a machine that has Docker. A full HiveMQ-style *Operator* (vs. plain
manifests/Kustomize) remains a legitimate but separate stretch goal: a
StatefulSet's native rolling-restart behavior already gets correct
one-at-a-time restarts with readiness gates for free; an operator earns its
keep for things Kubernetes doesn't do natively — hot-applying config to
running pods without restart (today: `SIGHUP` per pod via `kubectl exec`,
manual and not fanned out), coordinating simultaneous peer-list updates on
scale-up, fleet-wide cert rotation.

### Audit logging

HiveMQ emits structured audit events (connect, disconnect, auth failure,
ACL denial, config change, admin action) for SIEM export. Entmoot has
structured `tracing` logs for connect/disconnect/ACL-deny/auth-fail
already, and — again — workstream 6's `$meta/clients` bus gets us halfway
to a proper audit stream for free. Concrete, cheap next step: extend that
same event emission to also cover auth failures and ACL denials (small,
consistent addition to code that already exists), and document that the
rest (config changes, admin actions) rides on structured `tracing` logs
into whatever SIEM an operator points at — no new distributed mechanism
needed, since audit events are inherently local per-node emissions just
like everything else in this category.

### Multi-tenancy and quotas

`--scope` already gives full tenant *isolation* (no topic-namespace
collision) but no per-scope *quotas* — connection limits, publish rate,
retained count, and offline-queue depth are global per-node today, not
per-tenant. Fully local, no-coordination-needed fix: key the existing
counters by scope instead of globally, same shape as today's global ones. A
*global* cross-node quota ("tenant X gets 1000 connections total across
the whole mesh") is harder and probably not worth inventing a consensus
mechanism for — document per-node quotas as the pragmatic answer and let
an ingress/LB layer or capacity planning handle the rest.

### Where Entmoot already has a structural edge

Two places HiveMQ needs an extension to reach the same functionality
Entmoot's architecture gets closer to for free: **historian/long-term
storage** (HiveMQ bridges out to Kafka/InfluxDB/Timescale via extensions;
Entmoot's Phase 3 "Zenoh storage plugins for history/replay" can adopt an
existing Zenoh storage backend directly, since Zenoh already has a
first-class storage-plugin architecture) and **live client visibility**
(HiveMQ's Control Center needs cluster-aware RPC to show a mesh-wide client
list; Entmoot's `$meta/clients` bus gives that mesh-wide view to any
subscriber for free, no special API layer required).

### Priority order

1. ✅ Kubernetes packaging (Phase 2) — biggest, most overdue, blocked
   production credibility. Done: distroless image, StatefulSet + headless
   Service peer bootstrap, PDB/NetworkPolicy, dev/staging/production
   Kustomize overlays (`k8s/`, `k8s/README.md`). Still genuinely unverified
   against a live cluster — this environment has no Docker daemon or
   `kubectl`/`kind` — so the image build and `kubectl apply` steps need a
   real run on a machine with Docker before trusting this in production;
   the chaos manifests that depended on this landing can now target it for
   real instead of staying forward-looking.
2. ✅ Data Hub-style schema/behavior policy engine — done (v1: JSON Schema
   data policies + reconnect-churn as the one behavior-policy case that
   mattered most). Fully decentralization-compatible, no architectural
   risk shipped.
3. ✅ Pluggable/dynamic auth — done through hot-reload and static-key JWT;
   LDAP/AD bind and per-client dynamic permission placeholders remain open.
4. ✅ Control-center-lite (force-disconnect over a `@ctl/disconnect` Zenoh
   query, `ctl.rs`) and the audit-event extension (auth-fail/ACL-denials
   now on `$meta/clients` too) — both landed, both built directly on
   workstream 6. Still missing: the actual live dashboard UI consuming
   these (a frontend task, not a protocol one).
5. Per-scope quotas — moderate effort, purely local.
6. Session-state mesh replication (true session HA) — the hardest,
   most architecturally novel item; needs dedicated design time for the
   cross-node liveliness/ownership question, not a quick win.
7. MQTT 5 + shared subscriptions — protocol completeness; the cross-node
   shared-subscription load-balancing problem is a genuinely open design
   question, not a known-shape task.
8. Bridge/federation to Kafka or third-party brokers — moderate effort,
   lower urgency given Zenoh peering already covers Entmoot-to-Entmoot.
