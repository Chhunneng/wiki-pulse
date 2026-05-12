-- Rollup table for Streamlit (written by Spark foreachBatch JDBC / upsert)
CREATE TABLE IF NOT EXISTS rollup_minute (
    window_end   TIMESTAMPTZ NOT NULL,
    wiki         VARCHAR(256) NOT NULL,
    wiki_label   VARCHAR(512),
    edit_count   BIGINT       NOT NULL,
    bot_count    BIGINT       NOT NULL,
    human_count  BIGINT       NOT NULL,
    language     VARCHAR(32),
    anomaly_flag BOOLEAN      NOT NULL DEFAULT FALSE,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_end, wiki)
);

CREATE INDEX IF NOT EXISTS idx_rollup_window_end ON rollup_minute (window_end DESC);
-- Used by the dashboard's staleness probe (MAX(updated_at)).
CREATE INDEX IF NOT EXISTS idx_rollup_minute_updated_at ON rollup_minute (updated_at DESC);

COMMENT ON TABLE rollup_minute IS 'Windowed wiki edit counts; Spark appends/upserts every micro-batch.';

-- Tier 1: RecentChange action type breakdown (edit / new / log / …) plus minor-flag counts per wiki/minute bucket.
CREATE TABLE IF NOT EXISTS rollup_action_minute (
    window_end   TIMESTAMPTZ NOT NULL,
    wiki         VARCHAR(256) NOT NULL,
    event_type   VARCHAR(64)  NOT NULL,
    wiki_label   VARCHAR(512),
    language     VARCHAR(32),
    event_count  BIGINT       NOT NULL,
    bot_count    BIGINT       NOT NULL,
    human_count  BIGINT       NOT NULL,
    minor_count  BIGINT       NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_end, wiki, event_type)
);

CREATE INDEX IF NOT EXISTS idx_rollup_action_window_end
    ON rollup_action_minute (window_end DESC);
CREATE INDEX IF NOT EXISTS idx_rollup_action_minute_updated_at
    ON rollup_action_minute (updated_at DESC);

COMMENT ON TABLE rollup_action_minute IS 'Per RecentChange action type, bot/human/minor counts by wiki and minute window; Spark JDBC upsert.';
