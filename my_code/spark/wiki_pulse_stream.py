"""
Spark Structured Streaming: Kafka recentchange events → windowed aggregations
with broadcast join to static HDFS CSV (bonus), Parquet on HDFS (Hive-ready),
and Postgres upserts for Streamlit.

Uses two independent Kafka read streams (separate checkpoints) so we can write
rollup_minute and rollup_action_minute without forking a single streaming sink.
"""


from __future__ import annotations

import logging
import os
import subprocess
from datetime import timezone
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import execute_batch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-server:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "wiki.event.raw")
STARTING_OFFSETS = os.environ.get("KAFKA_STARTING_OFFSETS", "latest")
CHECKPOINT = os.environ.get(
    "WIKIPULSE_CHECKPOINT",
    "hdfs:///user/cloudera/wiki_pulse/checkpoints/main",
)
CHECKPOINT_ACTION = os.environ.get(
    "WIKIPULSE_CHECKPOINT_ACTION",
    "hdfs:///user/cloudera/wiki_pulse/checkpoints/action_by_type",
)
HDFS_OUT = os.environ.get(
    "WIKIPULSE_HIVE_WAREHOUSE",
    "hdfs:///user/cloudera/wiki_pulse/warehouse/wiki_window_agg",
)
# Parallel Parquet tasks under the same warehouse _temporary/ path can trigger HDFS lease /
# FileNotFoundException on small clusters. Default 1 = one task per foreachBatch write (slower, stable).
PARQUET_WRITE_COALESCE = max(1, int(os.environ.get("WIKIPULSE_PARQUET_WRITE_COALESCE", "1")))
LOOKUP_CSV = os.environ.get(
    "WIKIPULSE_LOOKUP_CSV",
    "hdfs:///data/static/wiki_domains.csv",
)
POSTGRES_URL = os.environ.get(
    "POSTGRES_URL",
    "postgresql://wikipulse:wikipulse_secret@metrics-db:5432/wiki_pulse_metrics",
)
PROCESSING_TRIGGER = os.environ.get("WIKIPULSE_TRIGGER", "30 seconds")
WATERMARK = os.environ.get("WIKIPULSE_WATERMARK", "2 minutes")
WINDOW_DURATION = os.environ.get("WIKIPULSE_WINDOW", "1 minute")
ANOM_THRESHOLD = int(os.environ.get("WIKIPULSE_ANOMALY_EDIT_THRESHOLD", "200"))

# Kafka source durability/back-pressure knobs. Override via env if traffic gets bursty.
KAFKA_MAX_OFFSETS_PER_TRIGGER = os.environ.get("KAFKA_MAX_OFFSETS_PER_TRIGGER", "50000")
KAFKA_FETCH_OFFSET_NUM_RETRIES = os.environ.get("KAFKA_FETCH_OFFSET_NUM_RETRIES", "5")
KAFKA_FETCH_OFFSET_RETRY_INTERVAL_MS = os.environ.get("KAFKA_FETCH_OFFSET_RETRY_INTERVAL_MS", "1000")
KAFKA_SESSION_TIMEOUT_MS = os.environ.get("KAFKA_SESSION_TIMEOUT_MS", "60000")
KAFKA_REQUEST_TIMEOUT_MS = os.environ.get("KAFKA_REQUEST_TIMEOUT_MS", "70000")
KAFKA_MAX_POLL_RECORDS = os.environ.get("KAFKA_MAX_POLL_RECORDS", "1000")

_EXPLAIN_COST_PRINTED = False

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    if logger.handlers:
        return
    level_name = os.environ.get("WIKIPULSE_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False


def _hdfs_abs_path(uri: str) -> str:
    """Map ``hdfs:///a/b.csv`` or ``hdfs://localhost:9000/a/b.csv`` to ``hdfs dfs`` path ``/a/b.csv``."""
    u = (uri or "").strip()
    if u.startswith("hdfs:///"):
        return "/" + u[len("hdfs:///") :].lstrip("/")
    if u.startswith("hdfs://"):
        path = urlparse(u).path
        return path if path else "/"
    return "/" + u.lstrip("/")


def _ensure_hdfs_lookup_exists(uri: str) -> None:
    """Fail fast with a runnable fix if the enrichment CSV disappeared (fresh HDFS volume, etc.)."""
    if not uri.lower().startswith("hdfs"):
        return
    cli_path = _hdfs_abs_path(uri)
    try:
        proc = subprocess.run(
            ["hdfs", "dfs", "-test", "-f", cli_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=120,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as ex:
        logger.warning("could not run hdfs dfs -test for lookup CSV (%s); continuing.", ex)
        return
    if proc.returncode != 0:
        raise RuntimeError(
            f"Lookup CSV missing on HDFS ({uri} → tested `{cli_path}`).\n"
            "Inside **main-bigdata-service** restore it, then restart streaming:\n"
            "  hdfs dfs -mkdir -p /data/static\n"
            f"  hdfs dfs -put -f /opt/my_code/static/wiki_domains.csv {cli_path}\n"
            "See README **Step 4** or **hdfs dfs -ls /data/static**."
        )


UPSERT_SQL = """
INSERT INTO rollup_minute (
    window_end, wiki, wiki_label, edit_count, bot_count, human_count, language, anomaly_flag
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (window_end, wiki) DO UPDATE SET
    wiki_label = EXCLUDED.wiki_label,
    edit_count = EXCLUDED.edit_count,
    bot_count = EXCLUDED.bot_count,
    human_count = EXCLUDED.human_count,
    language = EXCLUDED.language,
    anomaly_flag = EXCLUDED.anomaly_flag,
    updated_at = NOW()
"""

UPSERT_SQL_ACTION = """
INSERT INTO rollup_action_minute (
    window_end, wiki, wiki_label, language, event_type,
    event_count, bot_count, human_count, minor_count
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (window_end, wiki, event_type) DO UPDATE SET
    wiki_label = EXCLUDED.wiki_label,
    language = EXCLUDED.language,
    event_count = EXCLUDED.event_count,
    bot_count = EXCLUDED.bot_count,
    human_count = EXCLUDED.human_count,
    minor_count = EXCLUDED.minor_count,
    updated_at = NOW()
"""


# Only core RecentChange fields in from_json — strict nested types + minor mismatch
# (boolean vs int) can invalidate parsing. Minor flag comes from raw via get_json_object.
RAW_SCHEMA_LITE = StructType(
    [
        StructField("wiki", StringType(), True),
        StructField("type", StringType(), True),
        StructField("bot", BooleanType(), True),
        StructField("timestamp", DoubleType(), True),
    ]
)


def _pg_conninfo(url: str) -> str:
    if url.startswith("postgresql://"):
        from urllib.parse import urlparse

        p = urlparse(url)
        db = (p.path or "").lstrip("/")
        return (
            f"host={p.hostname} port={p.port or 5432} dbname={db} "
            f"user={p.username} password={p.password}"
        )
    return url


def _metrics_aux_tables_exist() -> None:
    """Fail fast before starting streams — avoids silent-empty queries if migrations were skipped."""
    required = ("rollup_action_minute",)
    conn = psycopg2.connect(_pg_conninfo(POSTGRES_URL))
    try:
        with conn.cursor() as cur:
            for tbl in required:
                cur.execute(
                    """
                    SELECT EXISTS (
                      SELECT FROM information_schema.tables
                      WHERE table_schema = 'public' AND table_name = %s
                    );
                    """,
                    (tbl,),
                )
                if not cur.fetchone()[0]:
                    raise RuntimeError(
                        f'Metrics Postgres is missing `{tbl}`. On the repo host run: '
                        f'docker compose exec -T metrics-db psql -U wikipulse -d wiki_pulse_metrics '
                        f'< my_code/schemas/metrics_addon_rollups.sql'
                    )
        logger.info("metrics aux table rollup_action_minute present")
    finally:
        conn.close()


def _as_utc_z(dt):
    if dt is None:
        return None
    try:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except AttributeError:
        return dt


def streaming_watermarked_recentchange(spark: SparkSession, lookup_dimensions_df):
    """One Kafka subscribe + parse + enrich + watermark.

    IMPORTANT: Give each parallel streaming pipeline its **own** ``F.broadcast(...)`` —
    sharing a single Broadcast object across Concurrent streaming queries breaks plans 2+ in practice.
    """
    global _EXPLAIN_COST_PRINTED

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        # Back-pressure: cap per-trigger batch so a backlog doesn't OOM the driver / starve heartbeats.
        .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSETS_PER_TRIGGER)
        # Retry transient fetch / offset errors instead of failing the streaming query.
        .option("fetchOffset.numRetries", KAFKA_FETCH_OFFSET_NUM_RETRIES)
        .option("fetchOffset.retryIntervalMs", KAFKA_FETCH_OFFSET_RETRY_INTERVAL_MS)
        # Pass-through Kafka consumer settings: longer session keeps the group alive between
        # micro-batches; smaller max.poll.records prevents one giant poll from blowing past heartbeats.
        .option("kafka.session.timeout.ms", KAFKA_SESSION_TIMEOUT_MS)
        .option("kafka.request.timeout.ms", KAFKA_REQUEST_TIMEOUT_MS)
        .option("kafka.max.poll.records", KAFKA_MAX_POLL_RECORDS)
        .load()
    )

    parsed = (
        kafka_df.select(F.col("value").cast("string").alias("raw"))
        .select(F.from_json(F.col("raw"), RAW_SCHEMA_LITE).alias("evt"), F.col("raw"))
        .select(
            F.col("evt.wiki").alias("wiki"),
            F.col("evt.type").alias("mw_type"),
            F.col("evt.bot").alias("bot"),
            F.col("evt.timestamp").alias("event_unix_ts"),
            F.lower(F.trim(F.coalesce(F.get_json_object(F.col("raw"), "$.minor"), F.lit("")))).alias("_minor_lc"),
        )
        .filter(F.col("wiki").isNotNull() & F.col("event_unix_ts").isNotNull())
        .withColumn(
            "event_unix_sec",
            F.when(F.col("event_unix_ts") > F.lit(1_000_000_000_000), F.col("event_unix_ts") / 1000.0).otherwise(
                F.col("event_unix_ts")
            ),
        )
        .withColumn("event_ts", F.to_timestamp(F.from_unixtime(F.col("event_unix_sec").cast("double"))))
        .filter(F.col("event_ts").isNotNull())
        .withColumn("bot", F.coalesce(F.col("bot"), F.lit(False)))
        .withColumn("is_minor", F.col("_minor_lc").isin("true", "1"))
        .drop("_minor_lc")
        # Spark 3.1 PySpark has no F.nullif; treat blank/whitespace-only as unknown.
        .withColumn("_etype_norm", F.trim(F.coalesce(F.col("mw_type"), F.lit(""))))
        .withColumn(
            "event_type",
            F.when(F.length(F.col("_etype_norm")) == F.lit(0), F.lit("unknown")).otherwise(
                F.col("_etype_norm")
            ),
        )
        .drop("mw_type", "_etype_norm")
    )

    lookup_bc_inner = F.broadcast(lookup_dimensions_df)

    enriched = (
        parsed.join(lookup_bc_inner, parsed.wiki == F.col("lk_wiki_key"), how="left")
        .withColumn(
            "wiki_label",
            F.coalesce(F.col("lk_wiki_label"), F.col("wiki")),
        )
        .withColumn(
            "language",
            F.coalesce(F.col("lk_language"), F.lit("unknown")),
        )
        .drop("lk_wiki_key", "lk_wiki_label", "lk_language")
    )

    if not _EXPLAIN_COST_PRINTED:
        enriched.explain("cost")
        _EXPLAIN_COST_PRINTED = True

    return enriched.withWatermark("event_ts", WATERMARK)


def write_postgres_upsert(batch_df, _batch_id: int) -> None:
    # limit(1).count() == 0 uses pure DataFrame ops; rdd.isEmpty() trips a known
    # PySpark 3.1.2 + Python 3.10+ bug (IndexError in rdd._wrap_function).
    if batch_df.limit(1).count() == 0:
        return
    # Minute rollups produce a few hundred rows at most, so .collect() is safest here.
    rows = batch_df.collect()
    conn = psycopg2.connect(_pg_conninfo(POSTGRES_URL))
    try:
        with conn.cursor() as cur:
            payload = [
                (
                    _as_utc_z(r["window_end"]),
                    r["wiki"],
                    r["wiki_label"],
                    int(r["edit_count"]),
                    int(r["bot_count"]),
                    int(r["human_count"]),
                    r["language"] or "",
                    bool(r["anomaly_flag"]),
                )
                for r in rows
            ]
            execute_batch(cur, UPSERT_SQL, payload, page_size=500)
        conn.commit()
    finally:
        conn.close()


def write_postgres_upsert_action(batch_df, _batch_id: int) -> None:
    if batch_df.limit(1).count() == 0:
        return
    rows = batch_df.collect()
    conn = psycopg2.connect(_pg_conninfo(POSTGRES_URL))
    try:
        with conn.cursor() as cur:
            payload = []
            for r in rows:
                et = str(r["event_type"] or "unknown").strip() or "unknown"
                et = et[:64]
                payload.append(
                    (
                        _as_utc_z(r["window_end"]),
                        r["wiki"],
                        r["wiki_label"],
                        r["language"] or "",
                        et,
                        int(r["event_count"]),
                        int(r["bot_count"]),
                        int(r["human_count"]),
                        int(r["minor_count"]),
                    )
                )
            execute_batch(cur, UPSERT_SQL_ACTION, payload, page_size=500)
        conn.commit()
    finally:
        conn.close()


def write_hive_parquet(batch_df, _batch_id: int) -> None:
    if batch_df.limit(1).count() == 0:
        return
    win_end = F.col("window_end").cast("timestamp")
    out = (
        batch_df.withColumn("window_end_ts", F.date_format(win_end, "yyyy-MM-dd HH:mm:ss"))
        .withColumn("ds", F.date_format(win_end, "yyyy-MM-dd"))
        .withColumn("hr", F.date_format(win_end, "HH"))
        .select(
            "window_end_ts",
            "wiki",
            "wiki_label",
            "edit_count",
            "bot_count",
            "human_count",
            "language",
            "anomaly_flag",
            "ds",
            "hr",
        )
    )
    # Serialize Parquet output tasks to reduce HDFS _temporary lease races (see README).
    out = out.coalesce(PARQUET_WRITE_COALESCE)
    out.write.mode("append").partitionBy("ds", "hr").format("parquet").save(HDFS_OUT)


def foreach_batch_main(batch_df, batch_id: int) -> None:
    write_hive_parquet(batch_df, batch_id)
    write_postgres_upsert(batch_df, batch_id)


def foreach_batch_action_only(batch_df, batch_id: int) -> None:
    write_postgres_upsert_action(batch_df, batch_id)


def build_spark() -> SparkSession:
    # On a laptop 8 partitions is plenty; bump SPARK_SQL_SHUFFLE_PARTITIONS to 64-200 on a
    # real cluster so the aggregation stage doesn't bottleneck on a single shuffle reader.
    shuffle_parts = os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "8")
    return (
        SparkSession.builder.appName("WikiPulseStructuredStreaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", shuffle_parts)
        .config("spark.sql.streaming.schemaInference", "true")
        # Exposes per-query metrics under /metrics — handy for Prometheus scraping at scale.
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )


def main() -> None:
    _configure_logging()
    spark = build_spark()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

    _metrics_aux_tables_exist()
    _ensure_hdfs_lookup_exists(LOOKUP_CSV)

    lookup_df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(LOOKUP_CSV)
        .select(
            F.col("wiki_key").alias("lk_wiki_key"),
            F.col("wiki_label").alias("lk_wiki_label"),
            F.col("language").alias("lk_language"),
        )
        .cache()
    )
    logger.info("Wiki domain lookup CSV cached lazily path=%s", LOOKUP_CSV)

    # Query 1 — totals (existing Parquet + rollup_minute)
    wm1 = streaming_watermarked_recentchange(spark, lookup_df)
    windowed_totals = (
        wm1.groupBy(
            F.window(F.col("event_ts"), WINDOW_DURATION),
            F.col("wiki"),
            F.col("wiki_label"),
            F.col("language"),
        )
        .agg(
            F.count(F.lit(1)).alias("edit_count"),
            F.sum(F.when(F.col("bot") == F.lit(True), 1).otherwise(0)).alias("bot_count"),
        )
        .withColumn("human_count", F.col("edit_count") - F.col("bot_count"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("anomaly_flag", F.col("edit_count") >= F.lit(ANOM_THRESHOLD))
        .select(
            "window_end",
            "wiki",
            "wiki_label",
            "language",
            "edit_count",
            "bot_count",
            "human_count",
            "anomaly_flag",
        )
    )

    # Query 2 — per action type + minor counts
    wm2 = streaming_watermarked_recentchange(spark, lookup_df)
    windowed_actions = (
        wm2.groupBy(
            F.window(F.col("event_ts"), WINDOW_DURATION),
            F.col("wiki"),
            F.col("wiki_label"),
            F.col("language"),
            F.col("event_type"),
        )
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.sum(F.when(F.col("bot") == F.lit(True), 1).otherwise(0)).alias("bot_count"),
            F.sum(F.when(F.col("is_minor") == F.lit(True), 1).otherwise(0)).alias("minor_count"),
        )
        .withColumn("human_count", F.col("event_count") - F.col("bot_count"))
        .withColumn("window_end", F.col("window.end"))
        .select(
            "window_end",
            "wiki",
            "wiki_label",
            "language",
            "event_type",
            "event_count",
            "bot_count",
            "human_count",
            "minor_count",
        )
    )

    q1 = (
        windowed_totals.writeStream.queryName("wiki_pulse_totals")
        .outputMode("append")
        .trigger(processingTime=PROCESSING_TRIGGER)
        .option("checkpointLocation", CHECKPOINT)
        .foreachBatch(foreach_batch_main)
        .start()
    )
    q2 = (
        windowed_actions.writeStream.queryName("wiki_pulse_actions")
        .outputMode("append")
        .trigger(processingTime=PROCESSING_TRIGGER)
        .option("checkpointLocation", CHECKPOINT_ACTION)
        .foreachBatch(foreach_batch_action_only)
        .start()
    )

    active = spark.streams.active
    logger.info("Spark active streaming queries: %s (expect 2)", len(active))
    logger.info(
        "Streaming queries started totals_id=%s checkpoint=%s parquet=%s; "
        "actions_id=%s checkpoint=%s",
        q1.id,
        CHECKPOINT,
        HDFS_OUT,
        q2.id,
        CHECKPOINT_ACTION,
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
