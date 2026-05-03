"""Single-run orchestration for the builder-costs refresh.

Steps:
    1. Init buildcost.db schema (idempotent).
    2. Sync local mirrors of buildcost / sde / primary market.
    3. Refresh build_watchlist from the primary market via SDE filter.
    4. Read jita_prices from the primary market local mirror.
    5. Fetch costs from EverRef for the buildable set.
    6. Upsert builder_costs to the buildcost remote.

Market-independent: the only market DB touched is the primary market,
purely as a source for ``watchlist`` and ``jita_prices``. The deployment
market is not consulted — its watchlist drift from primary is small enough
that the simplification is worth it.
"""

from __future__ import annotations

from dataclasses import dataclass

from mkts_backend.builder_costs.repository import (
    init_buildcost_tables,
    read_jita_prices,
    upsert_builder_costs,
)
from mkts_backend.builder_costs.watchlist_sync import refresh_build_watchlist
from mkts_backend.config.db_config import DatabaseConfig
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.esi.async_everref import run_async_fetch_builder_costs

logger = configure_logging(__name__)


@dataclass
class RunResult:
    success: bool
    fetched: int = 0
    missing: int = 0
    watchlist_size: int = 0


def run() -> RunResult:
    """Run a single end-to-end refresh of builder_costs in buildcost.db."""
    buildcost_db = DatabaseConfig("buildcost")
    sde_db = DatabaseConfig("sde")
    primary_db = DatabaseConfig("primary")

    init_buildcost_tables(buildcost_db)

    for db in (buildcost_db, sde_db, primary_db):
        if not db.verify_db_exists():
            logger.error(f"Database {db.alias} could not be initialized")
            return RunResult(success=False)

    items = refresh_build_watchlist(primary_db, sde_db, buildcost_db)
    if not items:
        logger.error("No buildable watchlist items; aborting builder cost refresh")
        return RunResult(success=False)

    type_ids = [item["type_id"] for item in items]
    watchlist_metadata = {
        item["type_id"]: {
            "type_id": item["type_id"],
            "type_name": item["type_name"],
            "group_name": item["group_name"],
            "category_id": item["category_id"],
        }
        for item in items
    }

    jita_prices = read_jita_prices(primary_db)

    sde_engine = sde_db.engine
    try:
        results = run_async_fetch_builder_costs(
            type_ids,
            jita_prices,
            sde_engine,
            watchlist_metadata=watchlist_metadata,
        )
    finally:
        sde_engine.dispose()

    if not results:
        logger.error("EverRef returned no successful results")
        return RunResult(success=False, watchlist_size=len(items))

    written = upsert_builder_costs(buildcost_db, list(results))
    missing = len(items) - written
    logger.info(
        f"Builder costs refresh complete: fetched={written}, "
        f"missing={missing}, watchlist_size={len(items)}"
    )
    return RunResult(
        success=True,
        fetched=written,
        missing=missing,
        watchlist_size=len(items),
    )
