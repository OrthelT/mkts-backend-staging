"""Mutation helpers for ``build_watchlist`` in buildcost.db.

build_watchlist is its own source of truth. wcmktprod.watchlist is mirrored
in via the existing ``add_watchlist`` command and reconciled on demand via
``build-watchlist mirror``. The cost refresh just *reads* from build_watchlist;
it doesn't rebuild it.

Three operations live here:

- ``add_to_build_watchlist`` — used by ``build-watchlist add`` and the
  auto-mirror in ``add_watchlist``. Applies the buildable filter unless
  ``force=True``.
- ``remove_from_build_watchlist`` — used by ``build-watchlist remove``.
- ``sync_from_market`` — used by ``build-watchlist mirror``. Adds market
  watchlist items missing from build_watchlist (buildable only, no force).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text

from mkts_backend.builder_costs.repository import (
    delete_build_watchlist_rows,
    read_build_watchlist_type_ids,
    upsert_build_watchlist,
)
from mkts_backend.builder_costs.sde_lookup import lookup_type_metadata
from mkts_backend.config.db_config import DatabaseConfig
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.esi.async_everref import filter_buildable

logger = configure_logging(__name__)


@dataclass
class AddResult:
    added: int = 0
    skipped: list[int] = field(default_factory=list)  # not buildable
    invalid: list[int] = field(default_factory=list)  # not in SDE


@dataclass
class RemoveResult:
    removed: int = 0
    not_present: list[int] = field(default_factory=list)


@dataclass
class SyncResult:
    market_size: int = 0
    already_present: int = 0
    added: int = 0
    skipped: list[int] = field(default_factory=list)


def add_to_build_watchlist(
    buildcost_db: DatabaseConfig,
    sde_db: DatabaseConfig,
    type_ids: list[int],
    *,
    force: bool = False,
) -> AddResult:
    """Add items to build_watchlist after looking up SDE metadata.

    With ``force=False``, items not produced by a manufacturing blueprint are
    skipped. With ``force=True``, only items missing from SDE entirely are
    rejected — buildable filter is bypassed.
    """
    if not type_ids:
        return AddResult()

    deduped = sorted(set(type_ids))
    metadata = lookup_type_metadata(deduped, sde_db)
    invalid = [tid for tid in deduped if tid not in metadata]
    candidates = [tid for tid in deduped if tid in metadata]

    if force:
        skipped: list[int] = []
        to_write = candidates
    else:
        buildable = filter_buildable(candidates, sde_db.engine)
        skipped = [tid for tid in candidates if tid not in buildable]
        to_write = [tid for tid in candidates if tid in buildable]

    if invalid:
        logger.warning(f"{len(invalid)} type_ids missing from SDE: {invalid[:10]}")
    if skipped:
        logger.warning(
            f"{len(skipped)} type_ids skipped (no manufacturing blueprint): "
            f"{skipped[:10]}{'…' if len(skipped) > 10 else ''}"
        )

    if not to_write:
        return AddResult(added=0, skipped=skipped, invalid=invalid)

    now = datetime.now(timezone.utc)
    rows = [
        {
            "type_id": tid,
            "type_name": metadata[tid]["type_name"],
            "group_name": metadata[tid]["group_name"],
            "category_id": metadata[tid]["category_id"],
            "added_at": now,
            "last_seen_at": now,
        }
        for tid in to_write
    ]
    written = upsert_build_watchlist(buildcost_db, rows)
    return AddResult(added=written, skipped=skipped, invalid=invalid)


def remove_from_build_watchlist(
    buildcost_db: DatabaseConfig,
    type_ids: list[int],
) -> RemoveResult:
    """Delete the given type_ids from build_watchlist. Idempotent."""
    if not type_ids:
        return RemoveResult()

    deduped = sorted(set(type_ids))
    present = read_build_watchlist_type_ids(buildcost_db)
    not_present = [tid for tid in deduped if tid not in present]
    to_delete = [tid for tid in deduped if tid in present]

    removed = delete_build_watchlist_rows(buildcost_db, to_delete)
    return RemoveResult(removed=removed, not_present=not_present)


def sync_from_market(
    buildcost_db: DatabaseConfig,
    sde_db: DatabaseConfig,
    market_db: DatabaseConfig,
) -> SyncResult:
    """Add market watchlist items missing from build_watchlist (buildable only)."""
    with market_db.engine.connect() as conn:
        df = pd.read_sql_query(text("SELECT type_id FROM watchlist"), conn)

    market_ids = {int(t) for t in df["type_id"].dropna().tolist()}
    if not market_ids:
        logger.warning(f"Market DB {market_db.alias} watchlist is empty")
        return SyncResult()

    already = read_build_watchlist_type_ids(buildcost_db)
    missing = sorted(market_ids - already)

    if not missing:
        logger.info(
            f"Sync: build_watchlist already covers all {len(market_ids)} market items"
        )
        return SyncResult(
            market_size=len(market_ids),
            already_present=len(market_ids),
        )

    add_result = add_to_build_watchlist(
        buildcost_db, sde_db, missing, force=False
    )
    return SyncResult(
        market_size=len(market_ids),
        already_present=len(market_ids) - len(missing),
        added=add_result.added,
        skipped=add_result.skipped,
    )
