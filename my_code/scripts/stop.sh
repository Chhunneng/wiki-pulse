#!/usr/bin/env bash
# Stop Spark streaming and pause the watchdog so it does not auto-restart.
#
# Run from repo root:
#   chmod +x my_code/scripts/stop.sh && ./my_code/scripts/stop.sh
#
# Resume the pipeline later with: ./my_code/scripts/start.sh
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

echo "== 1) Stop wiki-spark-watchdog so it does not resurrect Spark =="
docker compose -f "$COMPOSE_FILE" stop wiki-spark-watchdog 2>/dev/null \
  || echo "(watchdog not running — fine)"

echo ""
echo "== 2) Kill supervisor + spark-submit + driver inside main-bigdata-service =="
docker compose -f "$COMPOSE_FILE" exec -T main-bigdata-service bash -s <<'EOS'
set -eu
for pid_dir in /proc/[0-9]*; do
  pid="${pid_dir#/proc/}"
  case "$pid" in *[!0-9]*) continue ;; esac
  cmd=$(tr '\0' ' ' <"$pid_dir/cmdline" 2>/dev/null || true)
  [[ -z "$cmd" ]] && continue
  case "$cmd" in
    *spark_streaming_supervisor*)
      kill -TERM "$pid" 2>/dev/null || true ;;
    *SparkSubmit*|*spark-submit*)
      printf '%s' "$cmd" | grep -q '[w]iki_pulse_stream' && kill -TERM "$pid" 2>/dev/null || true ;;
    *python*|*Python*)
      [[ "$cmd" == *'/opt/my_code/spark/wiki_pulse_stream.py'* ]] && kill -TERM "$pid" 2>/dev/null || true ;;
  esac
done
sleep 2
# Force-kill stragglers.
for pid_dir in /proc/[0-9]*; do
  pid="${pid_dir#/proc/}"
  case "$pid" in *[!0-9]*) continue ;; esac
  cmd=$(tr '\0' ' ' <"$pid_dir/cmdline" 2>/dev/null || true)
  case "$cmd" in
    *spark_streaming_supervisor*|*SparkSubmit*|*spark-submit*|*wiki_pulse_stream*)
      kill -KILL "$pid" 2>/dev/null || true ;;
  esac
done
ps -eo pid,cmd --width 240 | \
  grep -E '[s]park_streaming_supervisor|[w]iki_pulse_stream|[S]parkSubmit' \
  || echo '(supervisor + Spark stopped or were not running.)'
EOS

echo ""
echo "Done. To resume:  ./my_code/scripts/start.sh"
