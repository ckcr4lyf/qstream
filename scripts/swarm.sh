#!/usr/bin/env bash
# qstream swarm management: start / status / stop
#
# Topology:
#   ffmpeg -> master (UDP 3333, HTTP 18080)
#             peer-1 (UDP 4444, HTTP 3333)   <- the one you watch
#             peer-2 (UDP 4445, HTTP 18081)
#
# Watch from home: ssh -L 3333:127.0.0.1:3333 <host>  then open
# http://localhost:3333/live.m3u8 in VLC / hls player.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/qstream
PIDS=./swarm.pids
MASTER_MANIFEST=./live/live.m3u8

start() {
    rm -f "$PIDS"
    setsid nohup "$BIN" server 3333 "$MASTER_MANIFEST" 18080 \
        </dev/null > /tmp/swarm_master.log 2>&1 &
    echo $! >> "$PIDS"
    sleep 0.4
    setsid nohup env QSTREAM_NAME=peer-1 "$BIN" peer 4444 127.0.0.1 3333 ./swarm-data/p1 3333 \
        </dev/null > /tmp/swarm_peer1.log 2>&1 &
    echo $! >> "$PIDS"
    sleep 0.4
    setsid nohup env QSTREAM_NAME=peer-2 "$BIN" peer 4445 127.0.0.1 3333 ./swarm-data/p2 18081 \
        </dev/null > /tmp/swarm_peer2.log 2>&1 &
    echo $! >> "$PIDS"
    echo "swarm started (pids: $(tr '\n' ' ' < "$PIDS"))"
}

status() {
    if [ ! -f "$PIDS" ]; then echo "swarm not running"; exit 0; fi
    while read -r pid; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "alive  $pid  $(ps -o args= -p "$pid" | sed 's/.*qstream/qstream/')"
        else
            echo "DEAD   $pid"
        fi
    done < "$PIDS"
}

stop() {
    [ -f "$PIDS" ] || { echo "swarm not running"; exit 0; }
    while read -r pid; do kill "$pid" 2>/dev/null || true; done < "$PIDS"
    sleep 0.5
    rm -f "$PIDS"
    echo "swarm stopped"
}

case "${1:-}" in
    start)  start ;;
    status) status ;;
    stop)   stop ;;
    *) echo "usage: $0 {start|status|stop}"; exit 1 ;;
esac
