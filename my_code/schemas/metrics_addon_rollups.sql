-- Apply on EXISTING metrics DB volumes where init scripts already ran once.
-- From repo root (use -T so Compose does not allocate a TTY when stdin is redirected):
--   docker compose exec -T metrics-db psql -U wikipulse -d wiki_pulse_metrics < my_code/schemas/metrics_addon_rollups.sql

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

-- Used by the dashboard's staleness probe on the main rollup table.
CREATE INDEX IF NOT EXISTS idx_rollup_minute_updated_at
    ON rollup_minute (updated_at DESC);
