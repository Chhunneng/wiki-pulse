"""Streamlit dashboard: reads rollup tables from metrics Postgres (Spark-populated)."""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError
from streamlit_autorefresh import st_autorefresh

REFRESH_SECS = float(os.environ.get("STREAMLIT_REFRESH_SECS", "15"))
DATABASE_URL = os.environ.get("DATABASE_URL", "")
DEFAULT_LOOKBACK_MIN = int(os.environ.get("WIKIPULSE_DASHBOARD_LOOKBACK_MINUTES", "10080"))
# If rollup_minute.updated_at stops moving, the dashboard looks frozen; warn viewers.
STALE_ROLLUP_MINUTES = float(os.environ.get("WIKIPULSE_DASHBOARD_STALE_MINUTES", "15"))

# Cache window: just under one auto-refresh tick so two reruns in the same tick share a hit.
_CACHE_TTL = max(1.0, REFRESH_SECS - 1.0)


def _rollup_staleness_age_minutes(last_upsert_val) -> float | None:
    if last_upsert_val is None or pd.isna(last_upsert_val):
        return None
    lu = pd.to_datetime(last_upsert_val, utc=True)
    now = pd.Timestamp.now(tz="UTC")
    return max(0.0, (now - lu).total_seconds() / 60.0)


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    """SQLAlchemy engine shared across reruns (connection pool reused, not re-created)."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    url = DATABASE_URL
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return create_engine(url, pool_pre_ping=True)


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_table_stats() -> pd.DataFrame:
    sql = text(
        """
        SELECT
            COUNT(*)::bigint AS total_rows,
            MAX(updated_at) AS last_upsert,
            MAX(window_end) AS newest_window_end
        FROM rollup_minute;
        """
    )
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_action_row_estimate() -> int | None:
    """Use pg_class.reltuples for an O(1) row count; falls back to None if the table is missing.

    Postgres updates reltuples on VACUUM/ANALYZE; for a streaming append-only table this stays
    close enough to the truth for a sidebar badge and is materially faster than COUNT(*).
    """
    sql = text(
        """
        SELECT reltuples::bigint AS rows
        FROM pg_class
        WHERE oid = to_regclass('public.rollup_action_minute');
        """
    )
    try:
        with get_engine().connect() as conn:
            row = conn.execute(sql).first()
    except SQLAlchemyError:
        return None
    if row is None or row[0] is None:
        return None
    return int(row[0])


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_rollups(limit_minutes: int | None = None) -> pd.DataFrame:
    lim = DEFAULT_LOOKBACK_MIN if limit_minutes is None else int(limit_minutes)
    sql = text(
        """
        SELECT window_end AT TIME ZONE 'UTC' AS window_end,
               wiki_label,
               wiki,
               edit_count,
               bot_count,
               human_count,
               anomaly_flag,
               updated_at
        FROM rollup_minute
        WHERE window_end >= NOW() - (:lim * INTERVAL '1 minute')
        ORDER BY window_end ASC;
        """
    )
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params={"lim": lim})


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_rollups_fallback(limit_rows: int = 2000) -> pd.DataFrame:
    """Safety net for clock skew (UTC events vs NOW() in another TZ). Indexed by updated_at."""
    sql = text(
        """
        SELECT window_end AT TIME ZONE 'UTC' AS window_end,
               wiki_label,
               wiki,
               edit_count,
               bot_count,
               human_count,
               anomaly_flag,
               updated_at
        FROM rollup_minute
        ORDER BY updated_at DESC
        LIMIT :n;
        """
    )
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params={"n": int(limit_rows)})


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_action_rollups(limit_minutes: int | None = None) -> pd.DataFrame:
    lim = DEFAULT_LOOKBACK_MIN if limit_minutes is None else int(limit_minutes)
    sql = text(
        """
        SELECT window_end AT TIME ZONE 'UTC' AS window_end,
               wiki_label,
               wiki,
               event_type,
               event_count,
               bot_count,
               human_count,
               minor_count,
               updated_at
        FROM rollup_action_minute
        WHERE window_end >= NOW() - (:lim * INTERVAL '1 minute')
        ORDER BY window_end ASC;
        """
    )
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params={"lim": lim})


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_action_rollups_fallback(limit_rows: int = 2000) -> pd.DataFrame:
    sql = text(
        """
        SELECT window_end AT TIME ZONE 'UTC' AS window_end,
               wiki_label,
               wiki,
               event_type,
               event_count,
               bot_count,
               human_count,
               minor_count,
               updated_at
        FROM rollup_action_minute
        ORDER BY updated_at DESC
        LIMIT :n;
        """
    )
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params={"n": int(limit_rows)})


def charts() -> None:
    df2 = pd.DataFrame()
    err = None
    used_fallback = False
    try:
        df2 = fetch_rollups()
        if df2.empty:
            stats = fetch_table_stats()
            total = int(stats["total_rows"].iloc[0]) if not stats.empty else 0
            if total > 0:
                df2 = fetch_rollups_fallback()
                used_fallback = True
    except Exception as ex:
        err = ex
    if err:
        st.error(str(err))
        return
    if df2.empty:
        st.info(
            "No rollup rows in metrics Postgres yet. The Spark watchdog should be bringing "
            "the streaming job up automatically — give it 1-2 minutes after `docker compose up -d`."
        )
        return
    if used_fallback:
        st.warning(
            f"No rows in last **{DEFAULT_LOOKBACK_MIN}** minutes (`window_end` filter). "
            f"Showing latest upserts anyway. Raise **WIKIPULSE_DASHBOARD_LOOKBACK_MINUTES** env if needed."
        )

    st.subheader("Total stream volume (all RecentChange types combined)")
    st.caption(
        "`rollup_minute.edit_count` counts every parsed event (edits, new pages, log actions, ...), "
        "not only `type=edit`."
    )

    with st.expander("Recent rollup rows — totals (latest 150 by bucket time)", expanded=True):
        tab = df2.sort_values("window_end", ascending=False).head(150).copy()
        st.dataframe(tab, width="stretch", hide_index=True)

    totals = df2.groupby("wiki_label", as_index=False)["edit_count"].sum()
    totals = totals.sort_values("edit_count", ascending=False).head(15)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Top wikis by event volume (sum in view)")
        st.bar_chart(totals.set_index("wiki_label"), width="stretch")
    with c2:
        st.subheader("Bot vs human (sum in view)")
        bh = df2.groupby("wiki_label", as_index=False).agg(
            {"bot_count": "sum", "human_count": "sum"}
        )
        bh = bh.sort_values("human_count", ascending=False).head(10)
        st.bar_chart(
            bh.set_index("wiki_label")[["bot_count", "human_count"]],
            width="stretch",
        )

    st.subheader("Events per time bucket (top wikis)")
    pivot = df2.pivot_table(
        index="window_end",
        columns="wiki_label",
        values="edit_count",
        aggfunc="sum",
        fill_value=0,
    )
    cols = pivot.sum(axis=0).nlargest(6).index.tolist()
    if cols:
        st.line_chart(pivot[cols], width="stretch")

    if df2["anomaly_flag"].any():
        st.subheader("Burst windows (threshold flag)")
        st.dataframe(
            df2.loc[df2["anomaly_flag"]]
            .sort_values("edit_count", ascending=False)
            .head(20),
            width="stretch",
        )

    st.divider()
    st.subheader("Richer metrics from RecentChange payload")
    st.caption(
        "Breakdown by **type** (edit / new / log / ...) and **minor** counts among `type=edit` events."
    )

    dfa_fallback = False
    try:
        dfa = fetch_action_rollups()
    except ProgrammingError as ex:
        st.warning(
            "Table `rollup_action_minute` is missing. Apply "
            "`my_code/schemas/metrics_addon_rollups.sql` and restart Spark. Detail: "
            f"{ex}"
        )
        dfa = pd.DataFrame()
    except Exception as ex:
        st.warning(f"Cannot read `rollup_action_minute`: {ex}")
        dfa = pd.DataFrame()

    if dfa.empty:
        na = fetch_action_row_estimate()
        if na is not None and na > 0:
            dfa = fetch_action_rollups_fallback(2000)
            dfa_fallback = True

    if dfa_fallback:
        st.warning(
            "No **`rollup_action_minute`** rows in the dashboard time window (`window_end` vs `NOW()`); "
            "showing latest rows by `updated_at` instead. If `rollup_minute` works but events are "
            "dated in UTC while `NOW()` is another TZ, consider raising "
            f"**WIKIPULSE_DASHBOARD_LOOKBACK_MINUTES** (currently **{DEFAULT_LOOKBACK_MIN}** min)."
        )

    if not dfa.empty:
        by_type_time = dfa.pivot_table(
            index="window_end",
            columns="event_type",
            values="event_count",
            aggfunc="sum",
            fill_value=0,
        )
        if not by_type_time.empty:
            st.subheader("Event mix over time (global sum by `type`)")
            st.area_chart(by_type_time, width="stretch")

        type_totals = dfa.groupby("event_type", as_index=False)["event_count"].sum()
        type_totals = type_totals.sort_values("event_count", ascending=False)
        c3, c4 = st.columns(2)
        with c3:
            st.subheader("Share of events by `type` (view sum)")
            st.bar_chart(
                type_totals.set_index("event_type")["event_count"],
                width="stretch",
            )

        edits = dfa[dfa["event_type"].fillna("").str.lower().eq("edit")]
        if not edits.empty:
            edg = edits.groupby("window_end", as_index=False).agg(
                {"minor_count": "sum", "event_count": "sum"}
            )
            edg["minor_share"] = edg["minor_count"] / edg["event_count"].clip(lower=1)
            with c4:
                st.subheader("Minor-edit share among `type=edit` (by minute bucket)")
                st.line_chart(
                    edg.set_index("window_end")[["minor_share"]],
                    width="stretch",
                )

        wiki_type = dfa.groupby(["wiki_label", "event_type"], as_index=False)["event_count"].sum()
        pivot_wt = wiki_type.pivot_table(
            index="wiki_label",
            columns="event_type",
            values="event_count",
            aggfunc="sum",
            fill_value=0,
        )
        if "log" in pivot_wt.columns and "edit" in pivot_wt.columns:
            st.subheader("Wikis with high `log` vs `edit` activity (sum in view)")
            lt = pivot_wt[["log", "edit"]].copy()
            lt["log_share_of_log_plus_edit"] = lt["log"] / (lt["log"] + lt["edit"]).clip(lower=1)
            show = lt.sort_values("log_share_of_log_plus_edit", ascending=False).head(12)
            st.dataframe(
                show.reset_index(),
                width="stretch",
                hide_index=True,
            )

        with st.expander("Raw `rollup_action_minute` rows (latest 200)"):
            st.dataframe(
                dfa.sort_values("window_end", ascending=False).head(200),
                width="stretch",
                hide_index=True,
            )

    else:
        ac = fetch_action_row_estimate()
        if ac is None:
            st.caption(
                "Could not read `rollup_action_minute` row count "
                "(migration not applied yet, or DB permission issue)."
            )
        elif ac == 0:
            st.info(
                "**`rollup_action_minute` is empty.** It is populated by Spark's second streaming "
                "query (`action_by_type` checkpoint). After applying "
                "`metrics_addon_rollups.sql`, restart Spark with `./my_code/scripts/start.sh` "
                "and watch the supervisor log for `actions_id=`."
            )
        else:
            st.warning(
                "Action rollup has rows in Postgres but none matched this page's `window_end` filter; "
                "try increasing **WIKIPULSE_DASHBOARD_LOOKBACK_MINUTES** or check DB/server clock skew."
            )


def main() -> None:
    st.set_page_config(page_title="Wiki Pulse Analytics", layout="wide")

    st_autorefresh(interval=int(REFRESH_SECS * 1000), limit=None, key="wikipulse_autorefresh")

    st.title("Wiki Pulse — near real-time edit activity")
    st.caption("Rollups populated by Spark Structured Streaming into metrics Postgres.")

    db_ok = True
    df = pd.DataFrame()
    try:
        df = fetch_rollups()
    except Exception as exc:
        db_ok = False
        st.error(f"Cannot read metrics DB: {exc}")

    side = st.sidebar
    side.header("Freshness")
    stats_df = pd.DataFrame()
    try:
        stats_df = fetch_table_stats()
    except Exception:
        stats_df = pd.DataFrame()

    if db_ok:
        side.caption(
            f"Main charts: `window_end` within last **{DEFAULT_LOOKBACK_MIN}** min "
            "(**WIKIPULSE_DASHBOARD_LOOKBACK_MINUTES**)."
        )

    if db_ok and not stats_df.empty and int(stats_df["total_rows"].iloc[0]) > 0:
        tr = int(stats_df["total_rows"].iloc[0])
        lu = stats_df["last_upsert"].iloc[0]
        nw = stats_df["newest_window_end"].iloc[0]
        side.success(f"**{tr}** rows in `rollup_minute`")
        side.write(f"Last UPSERT: **{lu}**")
        side.write(f"Newest `window_end`: **{nw}**")
        if not df.empty and "updated_at" in df.columns:
            last_spark = pd.to_datetime(df["updated_at"], utc=True).max()
            side.caption(f"Filtered view last update: {last_spark}")

        stale_m = _rollup_staleness_age_minutes(lu)
        if stale_m is not None and stale_m > STALE_ROLLUP_MINUTES:
            hint = (
                f"Rollups look **stale**: last `rollup_minute` UPSERT was about "
                f"**{stale_m:.0f} minutes ago** (> {STALE_ROLLUP_MINUTES:.0f} min threshold). "
                "Spark streaming has paused. `wiki-spark-watchdog` should re-launch it within "
                "30-60 s. If this banner persists for 2+ min, run:\n\n"
                "```\n./my_code/scripts/check.sh\n```\n\n"
                "to see watchdog logs and Spark supervisor status."
            )
            st.warning(hint)
            side.warning(f"Stale ~{stale_m:.0f} min — watchdog should recover automatically.")
    elif db_ok:
        side.warning("Table `rollup_minute` is empty — start producer + Spark streaming job.")

    if db_ok:
        ar = fetch_action_row_estimate()
        if ar is not None:
            side.caption(f"Also: ~**{ar}** `rollup_action_minute` rows (estimate).")

    side.metric("Dashboard refresh interval (s)", REFRESH_SECS)

    charts()


if __name__ == "__main__":
    main()
