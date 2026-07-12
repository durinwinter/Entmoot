#!/bin/sh
# Sweeps for the real path MTU on a link (e.g. inside a Nebula tunnel) using
# `ping -M do` (Don't Fragment), per RESILIENCE_ROADMAP.md workstream 5. Do
# this before trusting any benchmark number: Zenoh doesn't refuse an
# oversized batch, the underlying IP stack just silently fragments it (or
# drops it outright with DF-set paths), and either way the resulting
# drops/retransmits pollute every latency number until someone thinks to
# check this.
#
# Usage: scripts/mtu-sweep.sh <target-ip> [max_probe]
#
# Linux only (uses `ping -M do`; macOS's ping uses -D instead and isn't
# handled here).
set -eu

TARGET=${1:?"usage: mtu-sweep.sh <target-ip> [max_probe]"}
MAX=${2:-1500}
LOW=68 # smallest legal IPv4 MTU
HIGH=$MAX
BEST=0

command -v ping >/dev/null 2>&1 || {
    echo "ping not found" >&2
    exit 1
}

# Linux ping's -s is the ICMP payload size; the on-wire IPv4 packet is that
# plus 28 bytes (20-byte IP header + 8-byte ICMP header).
probe() {
    ping -c 1 -W 1 -M do -s "$1" "$TARGET" >/dev/null 2>&1
}

while [ "$LOW" -le "$HIGH" ]; do
    MID=$(((LOW + HIGH) / 2))
    if probe "$MID"; then
        BEST=$MID
        LOW=$((MID + 1))
    else
        HIGH=$((MID - 1))
    fi
done

MTU=$((BEST + 28))
cat <<EOF
Largest non-fragmenting ICMP payload to $TARGET: $BEST bytes
Path MTU (payload + 20-byte IP + 8-byte ICMP headers): $MTU bytes

Recommended zenoh_link_mtu: leave headroom for TCP/QUIC plus Nebula's own
encapsulation on top of this path MTU — a common conservative starting
point is $MTU minus roughly 60-100 bytes. Verify with
scripts/iperf3-fragmentation-check.sh before trusting benchmark numbers
over this link.
EOF
