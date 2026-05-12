#!/usr/bin/env bash
# Spark Streaming supervisor — runs INSIDE main-bigdata-service.
# Wraps spark-submit in an infinite loop with exponential backoff so the streaming driver
# self-heals from transient errors. Started/kept-alive from the host by wiki-spark-watchdog.
#
# Manual usage (inside main-bigdata-service):
#   nohup /opt/my_code/scripts/spark_streaming_supervisor.sh \
#     >> /opt/my_code/logs/spark_supervisor.log 2>&1 &
#
# Stop it cleanly: ./my_code/scripts/stop.sh from the host.

set -u

# ---------- Configuration ----------
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka-server:9092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-wiki.event.raw}"
export POSTGRES_URL="${POSTGRES_URL:-postgresql://wikipulse:wikipulse_secret@metrics-db:5432/wiki_pulse_metrics}"

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
if [[ ! -x "${SPARK_HOME}/bin/spark-submit" ]]; then
  SPARK_HOME="${SPARK_HOME:-/usr/hdp/current/spark2-client}"
fi

# Spark 3.1.2 ships in the lab image; spark-sql-kafka package must match.
SPARK_RUNTIME_VERSION="${SPARK_RUNTIME_VERSION:-3.1.2}"
PACKAGES="${WIKIPULSE_SPARK_PACKAGES:-org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_RUNTIME_VERSION},org.postgresql:postgresql:42.7.3}"

CK_MAIN_URI="${WIKIPULSE_CHECKPOINT:-hdfs:///user/cloudera/wiki_pulse/checkpoints/main}"
CK_ACTION_URI="${WIKIPULSE_CHECKPOINT_ACTION:-hdfs:///user/cloudera/wiki_pulse/checkpoints/action_by_type}"
WAREHOUSE_URI="${WIKIPULSE_HIVE_WAREHOUSE:-hdfs:///user/cloudera/wiki_pulse/warehouse/wiki_window_agg}"

LOG_DIR="/opt/my_code/logs"
SUPERVISOR_LOG="${LOG_DIR}/spark_supervisor.log"
SPARK_LOG="${LOG_DIR}/spark_streaming.log"
mkdir -p "${LOG_DIR}"

BACKOFF="${SUPERVISOR_BACKOFF_START_SECS:-5}"
BACKOFF_MAX="${SUPERVISOR_BACKOFF_MAX_SECS:-120}"
HEALTHY_RUNTIME_SECS="${SUPERVISOR_HEALTHY_RUNTIME_SECS:-300}"

ts() { date -Iseconds 2>/dev/null || date '+%Y-%m-%dT%H:%M:%S%z'; }
log() { printf '[supervisor %s] %s\n' "$(ts)" "$*" | tee -a "${SUPERVISOR_LOG}" >&2; }

# ---------- One-shot prerequisites ----------
# Spark driver needs psycopg2 to upsert rollups. The lab image ships without it; if the
# container got recreated, a previously apt-installed copy is gone. Try every strategy
# loudly so failures end up in the supervisor log instead of being hidden.
install_py_deps() {
  if python3 -c 'import psycopg2' >/dev/null 2>&1; then
    return 0
  fi
  log "psycopg2 missing - attempting install"

  # Strategy A: apt (Debian/Ubuntu) - the only path that avoids PEP 668 surprises.
  if command -v apt-get >/dev/null 2>&1; then
    log "trying apt-get install python3-psycopg2"
    apt-get install -y python3-psycopg2 >>"${SUPERVISOR_LOG}" 2>&1 \
      || apt-get update -y >>"${SUPERVISOR_LOG}" 2>&1 \
      && apt-get install -y python3-psycopg2 >>"${SUPERVISOR_LOG}" 2>&1 || true
    if python3 -c 'import psycopg2' >/dev/null 2>&1; then
      log "psycopg2 installed via apt"
      return 0
    fi
  fi

  # Strategy B: pip with --break-system-packages (newer pip + PEP 668).
  local req="/opt/my_code/spark/requirements-driver.txt"
  if [[ -f "$req" ]]; then
    for py in python3 python; do
      command -v "$py" >/dev/null 2>&1 || continue
      log "trying ${py} -m pip install --break-system-packages -r ${req}"
      "$py" -m pip install --break-system-packages -q -r "$req" >>"${SUPERVISOR_LOG}" 2>&1 \
        || "$py" -m pip install -q -r "$req" >>"${SUPERVISOR_LOG}" 2>&1 || true
      if python3 -c 'import psycopg2' >/dev/null 2>&1; then
        log "psycopg2 installed via ${py} pip"
        return 0
      fi
    done
  fi

  log "ERROR psycopg2 still not importable - Spark driver WILL fail with ImportError"
  log "manual fix: docker compose exec main-bigdata-service apt-get install -y python3-psycopg2"
  return 1
}

ensure_hdfs_dirs() {
  for uri in "${CK_MAIN_URI}" "${CK_ACTION_URI}" "${WAREHOUSE_URI}"; do
    local path="${uri#hdfs://}"
    if ! hdfs dfs -mkdir -p "${path}" >/dev/null 2>&1; then
      log "ERROR hdfs dfs -mkdir failed for ${path} (URI=${uri})"
      return 13
    fi
  done
}

ensure_lookup_csv() {
  # wiki_pulse_stream.py exits if /data/static/wiki_domains.csv is missing; self-heal here.
  if hdfs dfs -test -f /data/static/wiki_domains.csv 2>/dev/null; then
    return 0
  fi
  if [[ -f /opt/my_code/static/wiki_domains.csv ]]; then
    log "wiki_domains.csv missing on HDFS — uploading from /opt/my_code/static/"
    hdfs dfs -mkdir -p /data/static 2>/dev/null || true
    if hdfs dfs -put -f /opt/my_code/static/wiki_domains.csv /data/static/wiki_domains.csv; then
      log "wiki_domains.csv uploaded"
    else
      log "WARN upload failed — Spark will likely exit again"
    fi
  else
    log "WARN /opt/my_code/static/wiki_domains.csv not present in container"
  fi
}

run_spark_submit() {
  exec "${SPARK_HOME}/bin/spark-submit" \
    --master "${SPARK_MASTER:-local[*]}" \
    --packages "${PACKAGES}" \
    /opt/my_code/spark/wiki_pulse_stream.py
}

# ---------- Main loop ----------
trap 'log "SIGTERM received — exiting supervisor"; exit 0' TERM
trap 'log "SIGINT received — exiting supervisor"; exit 0' INT

log "supervisor pid=$$ starting"
ensure_hdfs_dirs || exit 13

while true; do
  # Re-check every iteration so the loop self-heals if you fix the container by hand.
  install_py_deps || log "WARN continuing despite psycopg2 install failure - Spark may crash"
  ensure_lookup_csv
  log "launching spark-submit"
  STARTED_AT=$(date +%s)
  ( run_spark_submit ) >> "${SPARK_LOG}" 2>&1
  RC=$?
  RAN_FOR=$(( $(date +%s) - STARTED_AT ))

  # If Spark stayed up long enough we treat the previous run as healthy and reset backoff.
  if (( RAN_FOR >= HEALTHY_RUNTIME_SECS )); then
    BACKOFF="${SUPERVISOR_BACKOFF_START_SECS:-5}"
  fi

  log "spark-submit exited rc=${RC} ran_for=${RAN_FOR}s — restart in ${BACKOFF}s"
  sleep "${BACKOFF}"
  BACKOFF=$(( BACKOFF * 2 ))
  (( BACKOFF > BACKOFF_MAX )) && BACKOFF="${BACKOFF_MAX}"
done
