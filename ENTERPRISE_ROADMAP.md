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
