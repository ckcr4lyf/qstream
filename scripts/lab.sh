#!/usr/bin/env bash
# tmux-based qstream lab: master + 5 peers with per-node fault injection.
#
#   lab.sh start <scenario>   start the swarm (tmux session "qstream-lab")
#   lab.sh stop               tear it down
#   lab.sh status             pane/process overview
#   lab.sh peers              dump GET /peers from every node
#   lab.sh attach             attach to the tmux session
#
# Ports: master UDP 3333 / HTTP 18080; peer-i UDP 4444+i-1, HTTP 3333 (p1)
# or 18080+i. Data dirs: /tmp/lab/<scenario>/p{1..5}. Logs: same dir.
set -euo pipefail
cd "$(dirname "$0")/.."

SESSION=qstream-lab
DIR=

start() {
    local SCEN=${1:-baseline}
    local DIR=/tmp/lab/$SCEN
    source "scripts/scenarios/$SCEN.env"
    mkdir -p "$DIR"/p{1..5}
    tmux kill-session -t "$SESSION" 2>/dev/null || true
    tmux new-session -d -s "$SESSION" -x 240 -y 50 -n master

    tmux send-keys -t "$SESSION:master" \
        "cd $(pwd) && $MASTER_FAULT QSTREAM_NAME=master ./target/release/qstream server 3333 ./live/live.m3u8 18080 2>&1 | tee $DIR/master.log; echo '=== MASTER EXITED ==='" Enter

    for i in 1 2 3 4 5; do
        tmux new-window -t "$SESSION" -n "peer$i"
        local port=$((4443 + i))
        local http=18080
        [ "$i" -eq 1 ] && http=3333 || http=$((18080 + i))
        tmux send-keys -t "$SESSION:peer$i" \
            "cd $(pwd) && $(eval echo \${PEER${i}_FAULT}) QSTREAM_NAME=peer-$i QSTREAM_RETENTION_SECS=60 ./target/release/qstream peer $port 127.0.0.1 3333 $DIR/p$i $http 2>&1 | tee $DIR/p$i.log; echo '=== PEER-$i EXITED ==='" Enter
    done

    tmux new-window -t "$SESSION" -n monitor
    local mon="while true; do clear; date +%T; echo '--- segments ---'; for i in 1 2 3 4 5; do printf 'p%s: %s\\n' \$i \$(ls $DIR/p\$i/seg_*.ts 2>/dev/null | wc -l); done; echo '--- peer-1 /peers ---'; curl -s --max-time 2 http://127.0.0.1:3333/peers 2>/dev/null | head -8; echo '--- master /peers ---'; curl -s --max-time 2 http://127.0.0.1:18080/peers 2>/dev/null | head -8; sleep 5; done"
    tmux send-keys -t "$SESSION:monitor" "$mon" Enter
    tmux select-window -t "$SESSION:master"
    echo "lab started: scenario=$SCEN (tmux session $SESSION, logs in $DIR)"
}

stop() {
    tmux kill-session -t "$SESSION" 2>/dev/null || true
    pkill -x qstream 2>/dev/null || true
    echo "lab stopped"
}

status() {
    if tmux has-session -t "$SESSION" 2>/dev/null; then
        echo "session $SESSION alive:"
        tmux list-windows -t "$SESSION" -F '  #W'
        pgrep -ax qstream | sed 's/^/  /'
    else
        echo "session $SESSION not running"
        pgrep -ax qstream | sed 's/^/  /' || true
    fi
}

peers() {
    for url in 127.0.0.1:3333 127.0.0.1:18080 127.0.0.1:18081 127.0.0.1:18082 127.0.0.1:18083 127.0.0.1:18084; do
        echo "=== $url/peers ==="
        curl -s --max-time 2 "http://$url/peers" || echo "(no response)"
        echo
    done
}

case "${1:-}" in
    start) start "${2:-baseline}" ;;
    stop) stop ;;
    status) status ;;
    peers) peers ;;
    attach) tmux attach -t "$SESSION" ;;
    *) echo "usage: lab.sh {start <scenario>|stop|status|peers|attach}"; exit 1 ;;
esac
