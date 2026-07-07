#!/bin/sh
# Spin up a local 3-node Entmoot mesh:
#   MQTT on 1883/1884/1885, Entmoot bus links on 7447/7448/7449.
# Node 2 and 3 connect to node 1; gossip completes the mesh.
set -eu
cd "$(dirname "$0")/.."

cargo build -p entmoot-node
BIN=target/debug/entmoot

trap 'kill 0' INT TERM EXIT

$BIN --id ent-1 --mqtt 127.0.0.1:1883 --bus-listen tcp/127.0.0.1:7447 &
$BIN --id ent-2 --mqtt 127.0.0.1:1884 --bus-listen tcp/127.0.0.1:7448 --peer tcp/127.0.0.1:7447 &
$BIN --id ent-3 --mqtt 127.0.0.1:1885 --bus-listen tcp/127.0.0.1:7449 --peer tcp/127.0.0.1:7447 &

echo ""
echo "3-node mesh up. Try, in two terminals:"
echo "  cargo run -p entmoot-node --example sub -- --port 1883 --topic 'plant/#'"
echo "  cargo run -p entmoot-node --example pub -- --port 1885 --topic plant/kiln1/temp --msg 993.5"
echo "Ctrl-C stops all nodes."
wait
