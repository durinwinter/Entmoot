#!/bin/sh
# Verifies full-size Entmoot-shaped payloads don't fragment once
# zenoh_link_mtu is set, per RESILIENCE_ROADMAP.md workstream 5. Run
# scripts/mtu-sweep.sh first to pick a real value.
#
# Usage: scripts/iperf3-fragmentation-check.sh <server-ip> [mtu]
# Run `iperf3 -s` on <server-ip> first.
set -eu

SERVER=${1:?"usage: iperf3-fragmentation-check.sh <server-ip> [mtu]"}
MTU=${2:-1200}

command -v iperf3 >/dev/null 2>&1 || {
    echo "iperf3 not found" >&2
    exit 1
}

echo "Sending $MTU-byte UDP datagrams to $SERVER for 10s..."
iperf3 -c "$SERVER" -u -l "$MTU" -b 50M -t 10

cat <<'EOF'

Check the output above (and the `iperf3 -s` log on the server) for:
  - lost-datagram / out-of-order percentages above your link's baseline —
    likely fragmentation-induced loss if -l is close to or above the real
    path MTU
  - jitter spikes correlated with -l size

If loss or jitter looks fragmentation-shaped, lower both zenoh_link_mtu and
the -l value here and re-run until it's clean, then use that value in
config.example.toml / --zenoh-link-mtu.
EOF
