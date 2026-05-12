#!/usr/bin/env bash
# Wipe metrics Postgres rollups + Spark streaming state (HDFS checkpoints + Parquet),
# then bring the pipeline back up with KAFKA_STARTING_OFFSETS=earliest so Spark replays
# the Kafka backlog once.
#
# Run from repo root:
#   chmod +x my_code/scripts/reset.sh && ./my_code/scripts/reset.sh
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

CK_MAIN="${WIKIPULSE_CHECKPOINT:-hdfs:///user/cloudera/wiki_pulse/checkpoints/main}"
CK_ACTION="${WIKIPULSE_CHECKPOINT_ACTION:-hdfs:///user/cloudera/wiki_pulse/checkpoints/action_by_type}"
WH="${WIKIPULSE_HIVE_WAREHOUSE:-hdfs:///user/cloudera/wiki_pulse/warehouse/wiki_window_agg}"

hdfs_path() { echo "${1#hdfs://}"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "== 1) Stop Spark streaming + watchdog =="
chmod +x "${SCRIPT_DIR}/stop.sh" 2>/dev/null || true
"${SCRIPT_DIR}/stop.sh"

echo ""
echo "== 2) Truncate metrics rollup tables =="
docker compose -f "$COMPOSE_FILE" exec -T metrics-db psql -U wikipulse -d wiki_pulse_metrics -v ON_ERROR_STOP=1 -c \
  "TRUNCATE TABLE rollup_minute, rollup_action_minute;"

echo ""
echo "== 3) Remove HDFS checkpoints + Parquet warehouse =="
docker compose -f "$COMPOSE_FILE" exec -T main-bigdata-service bash -lc "
  set -e
  for p in '$(hdfs_path "$CK_MAIN")' '$(hdfs_path "$CK_ACTION")' '$(hdfs_path "$WH")'; do
    hdfs dfs -rm -r -f \"\$p\" || true
  done
  hdfs dfs -mkdir -p '$(hdfs_path "$CK_MAIN")' '$(hdfs_path "$CK_ACTION")' '$(hdfs_path "$WH")'
  echo 'HDFS checkpoint + warehouse dirs recreated empty.'
"

echo ""
echo "== 4) Bring the pipeline back up (KAFKA_STARTING_OFFSETS=earliest) =="
chmod +x "${SCRIPT_DIR}/start.sh" 2>/dev/null || true
KAFKA_STARTING_OFFSETS=earliest "${SCRIPT_DIR}/start.sh"
