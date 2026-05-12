-- Run once in beeline / Hive CLI after Spark has written at least one Parquet partition.
-- Replace HDFS path with your deployment path (must match WIKIPULSE_HIVE_WAREHOUSE in spark job env).

CREATE DATABASE IF NOT EXISTS wiki_pulse;

USE wiki_pulse;

DROP TABLE IF EXISTS wiki_window_agg;

CREATE EXTERNAL TABLE wiki_window_agg (
    window_end_ts STRING,
    wiki STRING,
    wiki_label STRING,
    edit_count BIGINT,
    bot_count BIGINT,
    human_count BIGINT,
    language STRING,
    anomaly_flag BOOLEAN
)
PARTITIONED BY (ds STRING, hr STRING)
STORED AS PARQUET
-- Must match parquet base path produced by wiki_pulse_stream.py (WIKIPULSE_HIVE_WAREHOUSE env).
LOCATION 'hdfs:///user/cloudera/wiki_pulse/warehouse/wiki_window_agg'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- After new partitions arrive from Spark:
-- MSCK REPAIR TABLE wiki_window_agg;
