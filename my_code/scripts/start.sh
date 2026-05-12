#!/usr/bin/env bash
# Bring the Wiki Pulse pipeline up. Idempotent: safe to run any time.
#
# After `docker compose up -d`, the wiki-spark-watchdog will already auto-launch the Spark
# supervisor within ~60s. Run this script when you want to:
#   - Force-start Spark immediately (instead of waiting for the watchdog),
#   - Restart the producer after editing its code,
#   - Or replay the Kafka backlog (set KAFKA_STARTING_OFFSETS=earliest).
#
# Usage (from repo root):
#   chmod +x my_code/scripts/start.sh
#   ./my_code/scripts/start.sh
#
# Optional env:
#   KAFKA_STARTING_OFFSETS=latest      # default — only consume new Kafka messages
#   KAFKA_STARTING_OFFSETS=earliest    # replay everything in Kafka retention
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

KAFKA_STARTING_OFFSETS="${KAFKA_STARTING_OFFSETS:-latest}"

echo "== 1) Ensure HDFS dirs + lookup CSV (Spark fails fast if missing) =="
docker compose -f "$COMPOSE_FILE" exec -T main-bigdata-service bash -eo pipefail -c "
  hdfs dfs -mkdir -p /data/static \
    /user/cloudera/wiki_pulse/checkpoints/main \
    /user/cloudera/wiki_pulse/checkpoints/action_by_type \
    /user/cloudera/wiki_pulse/warehouse/wiki_window_agg
  if ! hdfs dfs -test -f /data/static/wiki_domains.csv 2>/dev/null; then
    hdfs dfs -put -f /opt/my_code/static/wiki_domains.csv /data/static/wiki_domains.csv
    echo 'Uploaded wiki_domains.csv to HDFS.'
  else
    echo 'wiki_domains.csv already on HDFS.'
  fi
"

echo ""
echo "== 2) Restart wiki-producer (fresh SSE -> Kafka) =="
docker compose -f "$COMPOSE_FILE" restart wiki-producer
sleep 3
docker compose -f "$COMPOSE_FILE" logs wiki-producer --tail 8

echo ""
echo "== 3) Launch Spark supervisor (auto-restart loop) — KAFKA_STARTING_OFFSETS=${KAFKA_STARTING_OFFSETS} =="
docker compose -f "$COMPOSE_FILE" exec -T \
  -e "KAFKA_STARTING_OFFSETS=${KAFKA_STARTING_OFFSETS}" \
  main-bigdata-service bash -s <<'EOS'
set -eu
mkdir -p /opt/my_code/logs
RUNNING=0
for pid_dir in /proc/[0-9]*; do
  pid="${pid_dir#/proc/}"
  case "$pid" in *[!0-9]*) continue ;; esac
  cmd=$(tr '\0' ' ' <"$pid_dir/cmdline" 2>/dev/null || true)
  [[ -z "$cmd" ]] && continue
  case "$cmd" in
    *spark_streaming_supervisor*)
      RUNNING=1 && break ;;
    *SparkSubmit*|*spark-submit*)
      printf '%s' "$cmd" | grep -q '[w]iki_pulse_stream' && RUNNING=1 && break ;;
    *python*|*Python*)
      [[ "$cmd" == *'/opt/my_code/spark/wiki_pulse_stream.py'* ]] && RUNNING=1 && break ;;
  esac
done
if [[ "$RUNNING" -eq 1 ]]; then
  echo 'Spark supervisor/driver already running — skipping. Use ./my_code/scripts/stop.sh first to force a restart.'
  tail -n 15 /opt/my_code/logs/spark_streaming.log 2>/dev/null || true
  exit 0
fi
nohup /opt/my_code/scripts/spark_streaming_supervisor.sh \
  >> /opt/my_code/logs/spark_supervisor.log 2>&1 &
sleep 8
echo "--- supervisor log tail ---"
tail -n 15 /opt/my_code/logs/spark_supervisor.log 2>/dev/null || true
echo "--- spark log tail ---"
tail -n 25 /opt/my_code/logs/spark_streaming.log 2>/dev/null || true
EOS

echo ""
echo "== 4) Ensure wiki-spark-watchdog is running =="
docker compose -f "$COMPOSE_FILE" up -d wiki-spark-watchdog

echo ""
echo "== 5) Postgres sanity check (wait 1-3 min if COUNT is still 0) =="
docker compose -f "$COMPOSE_FILE" exec -T metrics-db psql -U wikipulse -d wiki_pulse_metrics -c \
  "SELECT COUNT(*) AS rollup_minute_rows FROM rollup_minute;"

echo ""
echo 'Done. Open http://localhost:8501 after rollup_minute_rows grows.'
echo 'Diagnose anytime: ./my_code/scripts/check.sh'
