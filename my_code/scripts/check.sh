#!/usr/bin/env bash
# One stop diagnostic. Default mode prints a low-noise status summary; pass --verify to
# turn it into a strict smoke check that exits non-zero when something is wrong (handy in CI).
#
# Usage (from repo root):
#   ./my_code/scripts/check.sh            # status summary, exits 0
#   ./my_code/scripts/check.sh --verify   # strict mode, exits 1 on first failure
set -euo pipefail

MODE="summary"
for arg in "$@"; do
  case "$arg" in
    --verify|-v) MODE="verify" ;;
    -h|--help)
      grep -E '^# ' "$0" | sed 's/^# //'
      exit 0 ;;
    *) echo "Unknown arg: $arg" >&2; exit 2 ;;
  esac
done

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

COMPOSE=(docker compose -f "${COMPOSE_FILE}")

ok() { printf 'OK    %s\n' "$*"; }
warn() { printf 'WARN  %s\n' "$*" >&2; }
fail() {
  printf 'FAIL  %s\n' "$*" >&2
  if [[ "$MODE" == "verify" ]]; then
    exit 1
  fi
}

echo "== Containers =="
"${COMPOSE[@]}" ps --status running \
  main-bigdata-service kafka wiki-producer metrics-db wiki-streamlit wiki-spark-watchdog 2>/dev/null || true

echo ""
echo "== Watchdog log tail =="
"${COMPOSE[@]}" logs --tail 10 wiki-spark-watchdog 2>/dev/null || echo "(watchdog not deployed)"

echo ""
echo "== Spark supervisor + driver processes inside main-bigdata-service =="
if "${COMPOSE[@]}" exec -T main-bigdata-service bash -lc \
     'ps aux | grep -E "[s]park_streaming_supervisor|[w]iki_pulse_stream.py|[S]parkSubmit.*wiki_pulse_stream"' \
     2>/dev/null; then
  ok "supervisor / Spark driver running"
else
  fail "neither supervisor nor Spark driver is running (watchdog should fix this within ~30s)"
fi

echo ""
echo "== Spark progress markers (last 200 lines of spark_streaming.log) =="
spark_log_tail=$("${COMPOSE[@]}" exec -T main-bigdata-service tail -n 200 /opt/my_code/logs/spark_streaming.log 2>/dev/null || true)
if echo "$spark_log_tail" | grep -q 'Streaming queries started'; then
  ok "Spark streaming queries were registered"
else
  warn "no 'Streaming queries started' line yet — Spark is still warming up (first batch ~2-3 min after start)"
fi
if echo "$spark_log_tail" | grep -qE 'numInputRows" : [1-9]'; then
  ok "Spark micro-batches are receiving rows from Kafka"
elif echo "$spark_log_tail" | grep -q 'numInputRows" : 0'; then
  warn "Spark is running but micro-batches see 0 rows — producer/Kafka may be empty"
fi
echo "--- supervisor restart history (look for repeating ran_for=<low> = crash loop) ---"
"${COMPOSE[@]}" exec -T main-bigdata-service tail -n 10 /opt/my_code/logs/spark_supervisor.log 2>/dev/null \
  || echo "(no supervisor log yet)"

# If the supervisor is restarting Spark in under 60 s repeatedly, surface the actual
# error: the last Python traceback or "ERROR ..." line from the Spark log.
echo "--- last Spark error (if any) ---"
"${COMPOSE[@]}" exec -T main-bigdata-service bash -lc '
  log=/opt/my_code/logs/spark_streaming.log
  [ -f "$log" ] || { echo "(no spark log yet)"; exit 0; }
  # Show the last 30 lines surrounding the most recent "ERROR " or "Exception" or "Traceback".
  match=$(grep -nE "ERROR |Exception|Traceback|ImportError|RuntimeError" "$log" | tail -n 1 | cut -d: -f1)
  if [ -n "$match" ]; then
    start=$(( match > 5 ? match - 5 : 1 ))
    end=$(( match + 25 ))
    sed -n "${start},${end}p" "$log"
  else
    echo "(no ERROR / Exception lines in spark_streaming.log)"
  fi
' 2>/dev/null || true

echo ""
echo "== Producer activity =="
# Grep a wider window for any sign of life: startup line OR steady-state heartbeat OR
# the first-event marker. A long-running producer's startup line will have scrolled
# off a small tail, so we look across ~1500 lines.
producer_log_tail=$("${COMPOSE[@]}" logs wiki-producer --tail 1500 2>&1 || true)
if echo "$producer_log_tail" | grep -qE 'Kafka producer connected|heartbeat sent|First event queued'; then
  ok "wiki-producer is active (connect / heartbeat / first-event seen)"
else
  fail "wiki-producer has no connect / heartbeat / first-event log lines — try: docker compose logs wiki-producer --tail 100"
fi

echo ""
echo "== Kafka topic offsets =="
if "${COMPOSE[@]}" exec -T kafka sh -lc \
     'kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-server:9092 --topic wiki.event.raw --time -1 2>/dev/null | grep -q .'; then
  ok "wiki.event.raw offsets readable"
else
  fail "Kafka topic wiki.event.raw unreachable"
fi

echo ""
echo "== Metrics freshness =="
rows="$("${COMPOSE[@]}" exec -T metrics-db psql -U wikipulse -d wiki_pulse_metrics -qtAc \
  'SELECT COUNT(*)::text FROM rollup_minute;' 2>/dev/null | tr -d '[:space:]')"
if [[ "${rows:-}" =~ ^[0-9]+$ ]]; then
  if (( rows > 0 )); then
    ok "rollup_minute rows=${rows}"
  else
    fail "rollup_minute is empty — wait 1-3 min after fresh start, otherwise check Spark logs"
  fi
else
  fail "could not read rollup_minute count from metrics-db"
fi

"${COMPOSE[@]}" exec -T metrics-db psql -U wikipulse -d wiki_pulse_metrics -t -c \
  "SELECT NOW() AS now, MAX(updated_at) AS last_upsert, MAX(window_end) AS max_window_end FROM rollup_minute;" \
  2>/dev/null || true

echo ""
echo "== Streamlit reachability =="
if curl -sfS -o /dev/null --max-time 5 'http://127.0.0.1:8501/'; then
  ok "Streamlit responds at http://localhost:8501"
else
  fail "Streamlit not responding on localhost:8501 — docker compose logs wiki-streamlit"
fi

if [[ "$MODE" == "verify" ]]; then
  echo ""
  echo "PASS: end-to-end smoke checks."
fi
