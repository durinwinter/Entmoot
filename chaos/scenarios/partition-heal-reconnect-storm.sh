#!/bin/sh
# Replays the WS1 scenario turmoil couldn't run in-process (see
# RESILIENCE_ROADMAP.md workstream 1): partition the mesh, hold it, heal it,
# then storm the surviving node with simultaneous reconnects. Run
# chaos/toxiproxy-mesh.sh first, in another terminal, then this.
#
# Requires mosquitto_sub (mosquitto-clients package) as a zero-setup storm
# client. For the real benchmark — HdrHistogram-backed, coordinated-omission
# aware — use the workstream-4 harness once it lands instead of this.
set -eu

TOXIPROXY_ADDR=${TOXIPROXY_ADDR:-127.0.0.1:8474}
PROXY_NAME=${PROXY_NAME:-entmoot-a-link}
PARTITION_SECS=${1:-30}
CLIENTS=${2:-200}
NODE_B_PORT=${NODE_B_PORT:-1884}

command -v toxiproxy-cli >/dev/null 2>&1 || {
    echo "toxiproxy-cli not found: https://github.com/Shopify/toxiproxy" >&2
    exit 1
}
command -v mosquitto_sub >/dev/null 2>&1 || {
    echo "mosquitto_sub not found (mosquitto-clients package)" >&2
    exit 1
}

echo "Partitioning ($PROXY_NAME down) for ${PARTITION_SECS}s..."
toxiproxy-cli -h "$TOXIPROXY_ADDR" toggle "$PROXY_NAME"
sleep "$PARTITION_SECS"

echo "Healing ($PROXY_NAME back up)..."
toxiproxy-cli -h "$TOXIPROXY_ADDR" toggle "$PROXY_NAME"

echo "Firing $CLIENTS simultaneous reconnects at node-b (127.0.0.1:$NODE_B_PORT)..."
i=0
while [ "$i" -lt "$CLIENTS" ]; do
    mosquitto_sub -h 127.0.0.1 -p "$NODE_B_PORT" -i "storm-client-$i" -t 'plant/#' -C 1 -W 5 >/dev/null 2>&1 &
    i=$((i + 1))
done
wait

cat <<EOF
Storm fired. Check node-b's Prometheus /metrics for:
  entmoot_connect_shed_total      (admission control shedding the excess)
  entmoot_retained_scans_total vs entmoot_subscribes_total (coalescing ratio)
  entmoot_stale_retained_total    (partition-heal staleness flags, if
                                   retained_staleness_secs < ${PARTITION_SECS})
EOF
