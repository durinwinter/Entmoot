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

### Data Governance Hub (schema validation + behavior policies)

HiveMQ validates payloads against JSON Schema/Protobuf per topic filter
(pass/drop/reroute), and models client lifecycle as a state machine to
catch misbehaving clients (behavior policies) — a generalized, declarative
version of what Entmoot currently does as one-off hardcoded checks (ACLs,
publish rate limiting, connect admission, slow-consumer eviction). This is
the **most decentralization-friendly item on this whole list**: schema and
behavior validation are inherently per-message, per-connection, per-node
decisions — no cross-node coordination needed at all, unlike session HA
above. Not built yet. Shape it the same way `AclRule`/`StalenessRule`
already are (topic-filter-matched rule lists in config), with a schema
registry (JSON Schema to start; Protobuf is a bigger lift) and an action
pipeline (drop-and-log, reject-and-disconnect, reroute to a dead-letter
topic). Good next real feature — no architectural risk.

### Enterprise Security Extension (LDAP, OAuth2/JWT, RBAC, dynamic permissions)

Entmoot auth today is a static TOML file: SHA-256 password hashes, static
per-user ACL rules, mTLS CN identity — no LDAP/AD, no OAuth2/JWT, no
runtime reload (changing a user or ACL rule needs a restart), no per-client
dynamic permission templating, no certificate revocation checking
(CRL/OCSP). None of this needs cross-node coordination — auth and ACL
decisions are already, and always will be, a stateless per-connection
lookup evaluated locally by whichever node the client happens to land on.
The gap is entirely about *what the lookup source is*, not the
architecture. Practical order: (1) hot-reload the TOML file on SIGHUP or a
watched path — quick, fully decentralized-compatible, each node reloads
independently; (2) JWT/OAuth2 bearer validation against a JWKS endpoint on
CONNECT; (3) LDAP/AD bind; (4) per-client dynamic permission
placeholders as a config-schema extension; (5) CRL/OCSP checking for mTLS.

### Control Center (live admin UI + REST API)

HiveMQ's Control Center shows connected clients live and can force-
disconnect one via the UI/REST API. Entmoot's Canopy Console
(`web/index.html`) is a static config-authoring tool only — no live data —
though workstream 6 of [RESILIENCE_ROADMAP.md](RESILIENCE_ROADMAP.md) just
added the exact backend primitive a live one needs:
`$meta/clients/<node-id>/<client-id>` connect/subscribe/unsubscribe/
disconnect events, mesh-wide, the same way `$SYS` already works. This
actually makes a "control center" *easier* to build for Entmoot than for
HiveMQ, since no cluster-aware RPC layer is needed — a process that
subscribes to `$meta/clients/#` mesh-wide already has a live view of every
client on every node for free. What's still missing: a way to *act* on
that view (force-disconnect), which needs a request/response pattern — a
node publishes "disconnect client X" as a Zenoh query, whichever node
currently holds that client's live connection replies and acts. Natural
next step given `$meta/clients` already exists.

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

This is the single biggest, most concrete gap, and arguably matters more
than anything exotic above it: **Phase 2 packaging in PLAN.md is entirely
unimplemented** — no StatefulSet, no Helm/Kustomize, no distroless image,
no PodDisruptionBudget or NetworkPolicy. There's no standard way to
actually run Entmoot in production Kubernetes today, which undercuts every
other enterprise-parity claim (the workstream-3 Chaos Mesh manifests added
this session are explicitly written as forward-looking for exactly this
reason). Unlike everything else in this section, this is pure execution,
not new distributed-systems design — PLAN.md already scoped it. A
full HiveMQ-style *Operator* (vs. plain manifests/Helm) is a legitimate but
separate stretch goal: a StatefulSet's native rolling-restart behavior
already gets correct one-at-a-time restarts with readiness gates for free;
an operator earns its keep for things Kubernetes doesn't do natively —
hot-applying config to running pods without restart, coordinating
simultaneous peer-list updates on scale-up, fleet-wide cert rotation.

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

1. Kubernetes packaging (Phase 2) — biggest, most overdue, blocks
   production credibility and is already referenced as a dependency by
   work shipped this session.
2. Data Hub-style schema/behavior policy engine — fully
   decentralization-compatible, no architectural risk, real differentiator.
3. Pluggable/dynamic auth — hot-reload first, then JWT/OAuth2, then LDAP —
   fully decentralization-compatible.
4. Control-center-lite (force-disconnect over `$meta/clients`) and the
   audit-event extension — both cheap, both build directly on workstream 6.
5. Per-scope quotas — moderate effort, purely local.
6. Session-state mesh replication (true session HA) — the hardest,
   most architecturally novel item; needs dedicated design time for the
   cross-node liveliness/ownership question, not a quick win.
7. MQTT 5 + shared subscriptions — protocol completeness; the cross-node
   shared-subscription load-balancing problem is a genuinely open design
   question, not a known-shape task.
8. Bridge/federation to Kafka or third-party brokers — moderate effort,
   lower urgency given Zenoh peering already covers Entmoot-to-Entmoot.
