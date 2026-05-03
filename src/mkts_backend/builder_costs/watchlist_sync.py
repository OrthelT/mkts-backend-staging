"""Refresh build_watchlist from a market-DB watchlist.

Reads the configured market's ``watchlist`` table, joins SDE
``industryActivityProducts`` (activityID=1) to drop items with no
manufacturing blueprint, and upserts the result into ``buildcost.db``.

Returns the buildable rows so the runner can hand them straight to the
EverRef fetch without a second round-trip to buildcost.db.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text

from mkts_backend.builder_costs.repository import upsert_build_watchlist
from mkts_backend.config.db_config import DatabaseConfig
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.esi.async_everref import _get_meta_groups

logger = configure_logging(__name__)


def refresh_build_watchlist(
    market_db: DatabaseConfig,
    sde_db: DatabaseConfig,
    buildcost_db: DatabaseConfig,
) -> list[dict]:
    """Refresh build_watchlist from the given market's watchlist.

    Returns the buildable rows (the ones written to build_watchlist) — the
    runner uses these directly to drive the EverRef fetch.
    """
    with market_db.engine.connect() as conn:
        watchlist_df = pd.read_sql_query(
            text(
                "SELECT type_id, type_name, group_name, category_id FROM watchlist"
            ),
            conn,
        )

    if watchlist_df.empty:
        logger.warning(f"Market DB {market_db.alias} watchlist is empty")
        return []

    type_ids = [int(t) for t in watchlist_df["type_id"].dropna().tolist()]
    meta_groups = _get_meta_groups(type_ids, sde_db.engine)
    buildable = set(meta_groups.keys())

    now = datetime.now(timezone.utc)
    rows: list[dict] = []
    for record in watchlist_df.to_dict(orient="records"):
        tid = int(record["type_id"])
        if tid not in buildable:
            continue
        rows.append(
            {
                "type_id": tid,
                "type_name": record.get("type_name"),
                "group_name": record.get("group_name"),
                "category_id": int(record["category_id"])
                if pd.notna(record.get("category_id"))
                else None,
                "added_at": now,
                "last_seen_at": now,
            }
        )

    logger.info(
        f"Refreshing build_watchlist: {len(rows)} buildable / {len(type_ids)} total"
    )
    upsert_build_watchlist(buildcost_db, rows)
    return rows
