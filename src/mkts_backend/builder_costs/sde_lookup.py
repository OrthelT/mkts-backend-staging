"""SDE metadata lookup for the build_watchlist flow.

Uses ``sdetypes`` (the current canonical SDE type table per the Feb 2026
switch from ``inv_info``). Returns the four columns build_watchlist needs:
type_name, group_name, category_id, plus the type_id key.
"""

from __future__ import annotations

from sqlalchemy import text

from mkts_backend.config.db_config import DatabaseConfig
from mkts_backend.config.logging_config import configure_logging

logger = configure_logging(__name__)

_LOOKUP_CHUNK_SIZE = 500


def lookup_type_metadata(
    type_ids: list[int],
    sde_db: DatabaseConfig,
) -> dict[int, dict]:
    """Return ``{type_id: {type_name, group_name, category_id}}``.

    Type IDs absent from ``sdetypes`` are absent from the returned dict —
    callers treat them as invalid.
    """
    if not type_ids:
        return {}

    out: dict[int, dict] = {}
    with sde_db.engine.connect() as conn:
        for start in range(0, len(type_ids), _LOOKUP_CHUNK_SIZE):
            chunk = type_ids[start : start + _LOOKUP_CHUNK_SIZE]
            placeholders = ", ".join(f":t_{i}" for i, _ in enumerate(chunk))
            params = {f"t_{i}": tid for i, tid in enumerate(chunk)}
            query = text(
                f"""
                SELECT typeID, typeName, groupName, categoryID
                FROM sdetypes
                WHERE typeID IN ({placeholders})
                """
            )
            for row in conn.execute(query, params).mappings():
                tid = row.get("typeID")
                if tid is None:
                    continue
                out[int(tid)] = {
                    "type_name": row.get("typeName"),
                    "group_name": row.get("groupName"),
                    "category_id": int(row["categoryID"])
                    if row.get("categoryID") is not None
                    else None,
                }
    return out
