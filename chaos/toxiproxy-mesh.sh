#!/bin/sh
# Two-node Entmoot mesh with the inter-node bus link routed through Toxiproxy,
# so the link can be partitioned and healed on demand — the underlay-level
# tool the resilience plan calls for on links Chaos Mesh can't see (it
# operates inside a Kubernetes cluster network; a Nebula/cross-site tunnel,
# or here a plain loopback link standing in for one, lives below that).
#
# Requires toxiproxy-server + toxiproxy-cli: https://github.com/Shopify/toxiproxy
# (not installed by this repo; grab a release for your platform).
#
# node-b's --peer points at the toxiproxy listener instead of node-a
# directly, so the link is the only path between the two nodes (no gossip
# autoconnect bypass to worry about with just two nodes).
set -eu
cd "$(dirname "$0")/.."

command -v toxiproxy-server >/dev/null 2>&1 || {
    echo "toxiproxy-server not found: https://github.com/Shopify/toxiproxy" >&2
    exit 1
}
command -v toxiproxy-cli >/dev/null 2>&1 || {
    echo "toxiproxy-cli not found: https://github.com/Shopify/toxiproxy" >&2
    exit 1
}

cargo build -p entmoot-node
BIN=target/debug/entmoot

TOXIPROXY_ADDR=127.0.0.1:8474
PROXY_NAME=entmoot-a-link
PROXY_LISTEN=127.0.0.1:17447   # node-b's --peer dials this
UPSTREAM=127.0.0.1:7447        # node-a's real bus-listen address

trap 'kill 0' INT TERM EXIT

toxiproxy-server -host 127.0.0.1 -port 8474 >/tmp/toxiproxy.log 2>&1 &

for _ in $(seq 1 50); do
    curl -fs "http://$TOXIPROXY_ADDR/version" >/dev/null 2>&1 && break
    sleep 0.1
done

toxiproxy-cli -h "$TOXIPROXY_ADDR" create "$PROXY_NAME" --listen "$PROXY_LISTEN" --upstream "$UPSTREAM"

$BIN --id ent-a --mqtt 127.0.0.1:1883 --bus-listen "tcp/$UPSTREAM" &
$BIN --id ent-b --mqtt 127.0.0.1:1884 --bus-listen tcp/127.0.0.1:7448 --peer "tcp/$PROXY_LISTEN" &

cat <<EOF

Two-node mesh up: node-a on MQTT 1883, node-b on 1884.
node-b -> node-a bus link routed through toxiproxy ($TOXIPROXY_ADDR, proxy "$PROXY_NAME").

Partition the link:
  toxiproxy-cli -h $TOXIPROXY_ADDR toggle $PROXY_NAME
Heal it (same command — toggle again):
  toxiproxy-cli -h $TOXIPROXY_ADDR toggle $PROXY_NAME

Or drive a scripted partition/heal/reconnect-storm scenario:
  chaos/scenarios/partition-heal-reconnect-storm.sh [partition_secs] [num_clients]

Ctrl-C stops the mesh and toxiproxy.
EOF
wait
