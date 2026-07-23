# Running Entmoot on Kubernetes (Phase 2 packaging)

Everything in `k8s/` was written and verified as far as this development
environment allows: the Rust binary was cross-compiled to a genuinely static
musl target and the Kustomize overlays were rendered and their YAML content
inspected field-by-field. **The Docker image itself has not been built, and
none of this has been applied to a real cluster** — this sandbox has no
Docker daemon and no `kubectl`/`kind`. Treat the manifests as carefully
reasoned-about but unverified-in-anger, and work through this guide on your
Mac to actually prove it end to end. If something doesn't match reality,
the manifests are wrong, not you — please fix them and keep the comments
that explain *why* a shape was chosen (there are a few non-obvious ones).

## Prerequisites (macOS)

```sh
brew install --cask docker        # Docker Desktop — provides the daemon kind needs
brew install kind kubectl kustomize
open -a Docker                    # start the daemon, wait for it to say "running"
```

Verify before continuing:

```sh
docker info                       # must succeed, not just print a version
kind version
kubectl version --client
kustomize version
```

## 1. Create a local cluster

```sh
kind create cluster --name entmoot
kubectl cluster-info --context kind-entmoot
```

A single-node kind cluster is enough for the `dev` overlay (1 replica). The
`production` overlay's hard pod anti-affinity (one Entmoot pod per node)
needs a multi-node kind config if you want to test it locally:

```sh
cat <<'EOF' > /tmp/kind-multi-node.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
  - role: worker
  - role: worker
EOF
kind create cluster --name entmoot --config /tmp/kind-multi-node.yaml
```

(`production` runs 5 replicas; a 4-worker + 1-control-plane cluster is the
minimum that can schedule all 5 given the anti-affinity rule, since
kind's control-plane node is tainted against ordinary workloads by default.)

## 2. Build the image and load it into kind

kind runs its own containerd, isolated from your local Docker — an image
built with `docker build` is invisible to the cluster until you load it
explicitly.

```sh
cd /path/to/Entmoot
docker build -t entmoot:latest .
kind load docker-image entmoot:latest --name entmoot
```

The build is two-stage (see `Dockerfile`): a `rust:1-bookworm` builder
compiles `entmoot-node` against the `x86_64-unknown-linux-musl` target and
the build itself asserts the result is statically linked, then the binary
alone is copied into `gcr.io/distroless/static-debian12:nonroot` — no libc,
no shell, no package manager, runs as uid 65532. If you're on Apple
Silicon, Docker Desktop will build for your host arch (arm64) unless you
pass `--platform linux/amd64`; either works with kind as long as the image
tag matches what the manifests reference (`entmoot:latest` — the base
`kustomization.yaml`'s `images:` block only overrides the tag, not the
arch).

## 3. Create the config Secret

Every pod in the StatefulSet mounts the same file at
`/etc/entmoot/entmoot.toml` — copy the template, replace the placeholder
password hash, and generate a Secret from your copy (never commit the copy
with real credentials):

```sh
cp k8s/base/entmoot.example.toml /tmp/entmoot.toml
docker run --rm entmoot:latest --hash-password 'your-real-password' # paste the hash into /tmp/entmoot.toml

kubectl create namespace entmoot-dev   # or let the overlay's namespace.yaml do this via apply -k, see step 4
kubectl create secret generic entmoot-config \
  --namespace entmoot-dev \
  --from-file=entmoot.toml=/tmp/entmoot.toml
rm /tmp/entmoot.toml
```

If you uncomment the `[tls]` block in your copy for 8883, also create the
TLS Secret it references:

```sh
kubectl create secret generic entmoot-tls \
  --namespace entmoot-dev \
  --from-file=server.pem=/path/to/server.pem \
  --from-file=server.key=/path/to/server.key
```

and add a matching `volumeMounts`/`volumes` entry for it via an overlay
patch (the base manifests don't mount it, since TLS is optional).

## 4. Apply an overlay

Three overlays live under `k8s/overlays/`, differing by **namespace,
replica count, and resources only** — none of them use Kustomize's
`namePrefix`, deliberately, because the StatefulSet's peer-bootstrap DNS
names (`entmoot-0.entmoot-headless`, see below) are literal strings baked
into container `args`, and a name-prefix transform would silently break
them without kustomize knowing to rewrite those strings too.

```sh
kubectl apply -k k8s/overlays/dev
kubectl -n entmoot-dev get pods -w
```

| Overlay | Namespace | Replicas | Notes |
|---|---|---|---|
| `dev` | `entmoot-dev` | 1 | single pod, no mesh to form |
| `staging` | `entmoot-staging` | 3 | realistic sizing; run chaos scenarios here |
| `production` | `entmoot-production` | 5 | hard anti-affinity (1 pod/node), `LoadBalancer` client Service, `PodDisruptionBudget minAvailable: 3` |

If you created the Secret in a different namespace than the overlay
targets, `kubectl apply -k` will succeed but pods will fail to start
(`CreateContainerConfigError`, Secret not found) — the namespace in step 3
must match the overlay's namespace field.

## 5. Verify the mesh forms

With `staging` or `production` (more than one replica):

```sh
kubectl -n entmoot-staging get pods -o wide
kubectl -n entmoot-staging logs entmoot-1 | grep -i peer
```

Every pod is launched with the identical `--peer-zero
tcp/entmoot-0.entmoot-headless:7447` argument. `entmoot-0` itself detects
that this endpoint names its own pod (comparing the hostname label against
its own `--id`, which is `$(POD_NAME)` via the Downward API) and skips
adding itself as a peer; every other pod dials it as a seed link, and Zenoh
gossip fills in the rest of the mesh from there — no separate discovery
service, and no shell/entrypoint script needed even though the final image
has none. The headless Service (`entmoot-headless`) sets
`publishNotReadyAddresses: true` specifically so pods 1..N can resolve
`entmoot-0`'s DNS name even before pod 0 has passed its own readiness
probe, avoiding a startup deadlock.

Confirm client traffic end to end:

```sh
kubectl -n entmoot-staging port-forward svc/entmoot 1883:1883
# in another terminal:
mosquitto_sub -h localhost -p 1883 -t 'plant/#' -u plc1 -P 'your-real-password' &
mosquitto_pub -h localhost -p 1883 -t plant/kiln1/temp -m 993.5 -u plc1 -P 'your-real-password'
```

Check readiness/health directly if a pod won't go `Ready`:

```sh
kubectl -n entmoot-staging exec entmoot-0 -- true 2>&1 || true   # confirms no shell (expected, distroless)
kubectl -n entmoot-staging port-forward entmoot-0 9465:9465
curl localhost:9465/readyz   # "ready node=entmoot-0 zid=..."
curl localhost:9465/healthz  # "ok"
```

## 6. Config hot-reload without a restart

Users, ACLs, schema rules, and staleness settings can change without a pod
restart via `SIGHUP` (see `PLAN.md`):

```sh
kubectl create secret generic entmoot-config \
  --namespace entmoot-staging \
  --from-file=entmoot.toml=/tmp/entmoot-updated.toml \
  --dry-run=client -o yaml | kubectl apply -f -
# the kubelet's Secret sync can take up to ~1 minute to update the mounted file
kubectl -n entmoot-staging exec entmoot-0 -- kill -HUP 1
# repeat the exec per pod — there is no fan-out primitive for this in the base kubectl CLI
```

## 7. Layer chaos testing on top

Once a mesh is up under `staging`, `chaos/k8s/*.yaml` (Chaos Mesh
`NetworkChaos`/`Schedule` manifests — see `chaos/README.md`) replay the
partition/loss/latency scenarios from `RESILIENCE_ROADMAP.md` against the
real StatefulSet instead of a two-process stand-in:

```sh
# install Chaos Mesh once per cluster (not part of this repo's manifests)
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace \
  --set chaosDaemon.runtime=containerd \
  --set chaosDaemon.socketPath=/run/containerd/containerd.sock

# chaos/k8s/*.yaml assume namespace "entmoot" — edit the namespace and pod
# names (entmoot-0/1/2 vs the actual replica count) to match the overlay
# you applied, e.g. entmoot-staging with 3 replicas
kubectl apply -f chaos/k8s/networkchaos-site-partition.yaml
kubectl -n entmoot-staging get networkchaos
```

## Known gaps (tracked, not silently swept)

- **Session affinity through the client Service**: persistent sessions
  (`cleanSession=0`) aren't yet replicated between nodes (see
  `ENTERPRISE_ROADMAP.md`). A client reconnecting through the
  load-balanced `entmoot` Service can land on a different pod than before
  and lose its offline queue. There's no sticky-session fix here yet —
  either pin clients to a specific pod's DNS name directly
  (`entmoot-1.entmoot-headless:1883`) or wait for session replication.
- **No image registry step**: this guide loads the image straight into
  kind for local testing. A real cluster needs the image pushed to a
  registry the cluster can pull from, and `k8s/base/kustomization.yaml`'s
  `images:` block updated (or overridden per-overlay) to point at it.
- **NetworkPolicy enforcement depends on your CNI**: kind's default CNI
  (kindnet) does **not** enforce `NetworkPolicy` — `k8s/base/networkpolicy.yaml`
  will apply without error but silently do nothing on a stock kind cluster.
  Use a Calico or Cilium kind config if you want to actually test the
  policy, not just lint it.
