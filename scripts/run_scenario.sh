#!/usr/bin/env bash
# Run one fault scenario end-to-end: start lab -> wait -> (optional mid-run
# kill) -> collect -> summarize.
#   run_scenario.sh <name> [duration_seconds]
set -euo pipefail
cd "$(dirname "$0")/.."

NAME=$1
DUR=${2:-120}
DIR=/tmp/lab/$NAME
source "scripts/scenarios/$NAME.env"

echo "=== [$NAME] starting lab (${DUR}s) ==="
./scripts/lab.sh stop >/dev/null 2>&1 || true
rm -rf "$DIR"
./scripts/lab.sh start "$NAME"

KILLER=""
if [ -n "${KILL_AT:-}" ] && [ -n "${KILL_PATTERN:-}" ]; then
    ( sleep "$KILL_AT"; pgrep -f "$KILL_PATTERN" | xargs -r kill -9 2>/dev/null; \
      echo "killed $KILL_PATTERN at $(date +%T)" > "$DIR/kill.log" ) &
    KILLER=$!
    echo "  will kill '$KILL_PATTERN' at ${KILL_AT}s"
fi

sleep "$DUR"
[ -n "$KILLER" ] && kill "$KILLER" 2>/dev/null || true
# Snapshot the master's current playlist + segments BEFORE stopping, so
# metrics can compute end-coverage and replication lag against the state
# the scenario actually ended in (ffmpeg keeps producing afterwards).
curl -s --max-time 2 http://127.0.0.1:18080/live.m3u8 > "$DIR/master_playlist.m3u8" 2>/dev/null || true
ls -l --time-style=+%s live/seg_*.ts > "$DIR/master_segs.txt" 2>/dev/null || true
./scripts/lab.sh stop >/dev/null 2>&1 || true

echo "=== [$NAME] results ==="
python3 scripts/metrics.py "$DIR" | tee "$DIR/summary.txt"
echo "=== [$NAME] done (logs in $DIR) ==="
