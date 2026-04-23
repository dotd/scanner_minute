#!/usr/bin/env bash
# Supervisor for scanner_minute container.
# Runs three processes:
#   1. Node server   (dashboard + OAuth)
#   2. Realtime scanner  (posts breakouts/news/candles to Node)
#   3. Industries loop  (runs the industries scanner once an hour)
#
# On SIGTERM/SIGINT, all three are killed and the container exits.

set -u

cd /app

mkdir -p /app/logs /app/data /app/data/industries_reports /app/data/rocksdict_snapshots

node /app/node_server/server.js &
NODE_PID=$!
echo "[supervisor] node server pid=${NODE_PID}"

# Give Node a moment to bind before the scanner starts POSTing.
sleep 2

python /app/tst/realtime_scanner/tst_scan_realtime.py \
    ${REALTIME_SCANNER_ARGS:-} &
SCANNER_PID=$!
echo "[supervisor] realtime scanner pid=${SCANNER_PID}"

(
    while true; do
        echo "[supervisor] running industries scanner..."
        python /app/tst/industries_scanner/tst_industries_scanner.py || \
            echo "[supervisor] industries scanner exited with $?"
        sleep "${INDUSTRIES_INTERVAL_SECS:-3600}"
    done
) &
HOURLY_PID=$!
echo "[supervisor] industries loop pid=${HOURLY_PID}"

shutdown() {
    echo "[supervisor] shutting down..."
    kill -TERM "${NODE_PID}" "${SCANNER_PID}" "${HOURLY_PID}" 2>/dev/null || true
    wait "${NODE_PID}" "${SCANNER_PID}" "${HOURLY_PID}" 2>/dev/null || true
    exit 0
}
trap shutdown TERM INT

wait -n "${NODE_PID}" "${SCANNER_PID}" "${HOURLY_PID}"
EXIT_CODE=$?
echo "[supervisor] a child exited with ${EXIT_CODE}; shutting down siblings"
shutdown
