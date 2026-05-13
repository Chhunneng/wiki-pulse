#!/usr/bin/env bash
# One-shot Spark Structured Streaming driver — runs INSIDE main-bigdata-service.
# Prerequisites + single spark-submit (no restart loop).
#
# From the repo host, follow README **Run the pipeline** (docker compose exec …).
# Manual run inside the container:
#   chmod +x /opt/my_code/scripts/run_spark_stream.sh   # if the bind mount is not executable
#   nohup /opt/my_code/scripts/run_spark_stream.sh >> /opt/my_code/logs/spark_streaming.log 2>&1 &
#
# To stop Spark, see README **Stop Spark streaming**.

set -eu

export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka-server:9092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-wiki.event.raw}"
export POSTGRES_URL="${POSTGRES_URL:-postgresql://wikipulse:wikipulse_secret@metrics-db:5432/wiki_pulse_metrics}"

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
if [[ ! -x "${SPARK_HOME}/bin/spark-submit" ]]; then
  SPARK_HOME="${SPARK_HOME:-/usr/hdp/current/spark2-client}"
fi

SPARK_RUNTIME_VERSION="${SPARK_RUNTIME_VERSION:-3.1.2}"
PACKAGES="${WIKIPULSE_SPARK_PACKAGES:-org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_RUNTIME_VERSION},org.postgresql:postgresql:42.7.3}"

CK_MAIN_URI="${WIKIPULSE_CHECKPOINT:-hdfs:///user/cloudera/wiki_pulse/checkpoints/main}"
CK_ACTION_URI="${WIKIPULSE_CHECKPOINT_ACTION:-hdfs:///user/cloudera/wiki_pulse/checkpoints/action_by_type}"
WAREHOUSE_URI="${WIKIPULSE_HIVE_WAREHOUSE:-hdfs:///user/cloudera/wiki_pulse/warehouse/wiki_window_agg}"

LOG_DIR="/opt/my_code/logs"
SPARK_LOG="${LOG_DIR}/spark_streaming.log"
mkdir -p "${LOG_DIR}"

ts() { date -Iseconds 2>/dev/null || date '+%Y-%m-%dT%H:%M:%S%z'; }
log() { printf '[run_spark_stream %s] %s\n' "$(ts)" "$*" >&2; }

install_py_deps() {
  if python3 -c 'import psycopg2' >/dev/null 2>&1; then
    return 0
  fi
  log "psycopg2 missing — attempting install"
  if command -v apt-get >/dev/null 2>&1; then
    apt-get install -y python3-psycopg2 >>"${SPARK_LOG}" 2>&1 \
      || apt-get update -y >>"${SPARK_LOG}" 2>&1 \
      && apt-get install -y python3-psycopg2 >>"${SPARK_LOG}" 2>&1 || true
    if python3 -c 'import psycopg2' >/dev/null 2>&1; then
      log "psycopg2 installed via apt"
      return 0
    fi
  fi
  local req="/opt/my_code/spark/requirements-driver.txt"
  if [[ -f "$req" ]]; then
    for py in python3 python; do
      command -v "$py" >/dev/null 2>&1 || continue
      "$py" -m pip install --break-system-packages -q -r "$req" >>"${SPARK_LOG}" 2>&1 \
        || "$py" -m pip install -q -r "$req" >>"${SPARK_LOG}" 2>&1 || true
      if python3 -c 'import psycopg2' >/dev/null 2>&1; then
        log "psycopg2 installed via ${py} pip"
        return 0
      fi
    done
  fi
  log "ERROR psycopg2 still not importable — driver will fail; try: apt-get install -y python3-psycopg2"
  return 1
}

ensure_hdfs_dirs() {
  for uri in "${CK_MAIN_URI}" "${CK_ACTION_URI}" "${WAREHOUSE_URI}"; do
    local path="${uri#hdfs://}"
    if ! hdfs dfs -mkdir -p "${path}" >/dev/null 2>&1; then
      log "ERROR hdfs dfs -mkdir failed for ${path}"
      return 13
    fi
  done
}

ensure_lookup_csv() {
  if hdfs dfs -test -f /data/static/wiki_domains.csv 2>/dev/null; then
    return 0
  fi
  if [[ -f /opt/my_code/static/wiki_domains.csv ]]; then
    log "wiki_domains.csv missing on HDFS — uploading from /opt/my_code/static/"
    hdfs dfs -mkdir -p /data/static 2>/dev/null || true
    hdfs dfs -put -f /opt/my_code/static/wiki_domains.csv /data/static/wiki_domains.csv \
      && log "wiki_domains.csv uploaded" \
      || log "WARN upload failed"
  else
    log "WARN /opt/my_code/static/wiki_domains.csv not present in container"
  fi
}

log "pid=$$ starting"
ensure_hdfs_dirs || exit 13
install_py_deps || log "WARN continuing without psycopg2 — Spark may crash"
ensure_lookup_csv
log "launching spark-submit"

exec "${SPARK_HOME}/bin/spark-submit" \
  --master "${SPARK_MASTER:-local[*]}" \
  --packages "${PACKAGES}" \
  /opt/my_code/spark/wiki_pulse_stream.py
