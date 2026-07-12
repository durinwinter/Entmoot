# Chaos testing (workstream 3)

Two layers, matching where each kind of link actually lives (see
[RESILIENCE_ROADMAP.md](../RESILIENCE_ROADMAP.md) workstream 3):

## Toxiproxy — underlay links, runnable today

`toxiproxy-mesh.sh` starts a real two-node Entmoot mesh (built from this
repo, no Kubernetes needed) with the inter-node bus link routed through
[Toxiproxy](https://github.com/Shopify/toxiproxy), so you can partition and
heal that link on demand:

```sh
chaos/toxiproxy-mesh.sh
# in another terminal:
chaos/scenarios/partition-heal-reconnect-storm.sh 30 200   # 30s partition, 200-client storm
```

This replays the exact scenario workstream 1 wanted `turmoil` for
("partition 30s, heal, 2,000 simultaneous reconnects") but couldn't run
in-process, because Zenoh owns its own transport/runtime internals that
aren't turmoil-aware. Toxiproxy gets the same scenario shape by sitting at
the real TCP layer instead — this is also the tool of record for
Nebula-specific paths (UDP-in-UDP, hole-punch recovery) once those exist,
since Chaos Mesh (below) only sees inside the Kubernetes cluster network,
and a cross-site tunnel lives below that.

The scenario script uses `mosquitto_sub` as a zero-setup storm client. For
rigorous measurement (HdrHistogram, coordinated-omission correction) use the
workstream-4 benchmark harness once it lands instead.

## Chaos Mesh — cluster-level, assumes Phase 2 packaging

`k8s/*.yaml` are [Chaos Mesh](https://chaos-mesh.org) `NetworkChaos` /
`Schedule` manifests: a 1-vs-2 site partition, packet loss, and latency/
jitter injection against an Entmoot StatefulSet. These assume the Phase 2
Kubernetes packaging described in [PLAN.md](../PLAN.md) (a StatefulSet named
`entmoot`, labelled `app=entmoot`) which hasn't shipped yet — adjust
selectors/namespace to your actual deployment. They're declared as YAML and
schedulable, fitting a Kubernetes-native-from-day-one posture, and they're
meant to replay the same partition/loss/latency shapes as the Toxiproxy
scripts above, against the real stack instead of a two-process stand-in.

| File | What it does |
|---|---|
| `networkchaos-site-partition.yaml` | Splits `entmoot-0` from `entmoot-{1,2}` for 60s, heals automatically |
| `networkchaos-loss.yaml` | 10% packet loss (25% correlated) on all bus links for 60s |
| `networkchaos-latency.yaml` | 50ms ± 10ms latency on all bus links for 120s |
| `schedule-partition-matrix.yaml` | Recurring partition experiment for unattended soak testing |

Apply with `kubectl apply -f chaos/k8s/networkchaos-site-partition.yaml`
once Chaos Mesh is installed and Phase 2 packaging is deployed.
