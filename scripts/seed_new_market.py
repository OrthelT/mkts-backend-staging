"""Seed a new market's remote Turso DB with reference/config tables.

Copies the market-independent "reference" tables (watchlist, doctrines, fits,
etc.) from a source market's LOCAL database into a destination market's REMOTE
(Turso cloud) database. Market-data tables (marketorders, market_history,
marketstats, jita_prices, esi_request_cache, update_log) are intentionally NOT
copied — those are populated by the normal collection run for the new market.

Use this to bootstrap a freshly-created market so the frontend has doctrines,
watchlist, and targets to display before the first data-collection run.

Flow per table (one transaction each, on the destination remote engine):
  1. Read source rows (model column set) from <source> local DB.
  2. Ensure schema: create any missing target tables from Base (checkfirst).
  3. CSV backup of current destination rows (if any).
  4. DELETE all destination rows, then INSERT the source rows (ids preserved
     so cross-table references stay consistent).
  5. Verify destination row count == source row count, else roll back.

Writes to the destination CLOUD only. The destination's local .db mirror stays
stale until a subsequent `mkts-backend sync`.

Dry-run by default. Pass --apply to actually write.

Examples:
  # Default: copy wcmktnewkeep -> wcmktbkg (dry-run, shows the plan)
  uv run python scripts/seed_new_market.py

  # Actually write
  uv run python scripts/seed_new_market.py --apply

  # A different source/destination, only two tables
  uv run python scripts/seed_new_market.py --source wcmktnewkeep --dest wcmktbkg \\
      --only watchlist --only doctrines --apply
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from mkts_backend.config.db_config import DatabaseConfig
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.db.models import (
    Base,
    Doctrines,
    DoctrineFitItems,
    DoctrineMap,
    LeadShips,
    ModuleEquivalents,
    ShipTargets,
    Watchlist,
)

logger = configure_logging(__name__)

BACKUP_DIR = Path("data/migration_backups")

# Reference/config tables to seed into a new market, in dependency-friendly
# order. Add a model here to include another reference table in the bootstrap.
# Market-data tables are deliberately excluded (see module docstring).
REFERENCE_MODELS = [
    Watchlist,
    Doctrines,
    DoctrineFitItems,
    DoctrineMap,
    LeadShips,
    ShipTargets,
    ModuleEquivalents,
]


def table_exists(engine, table: str) -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name=:t"),
            {"t": table},
        ).fetchone()
    return row is not None


def read_source_rows(src: DatabaseConfig, table: str, cols: list[str]) -> list[dict]:
    """Read the model's columns from the source LOCAL db as a list of dicts."""
    col_list = ", ".join(cols)
    with src.engine.connect() as conn:
        rows = conn.execute(text(f"SELECT {col_list} FROM {table}")).fetchall()
    return [dict(r._mapping) for r in rows]


def backup_table(dest: DatabaseConfig, table: str) -> Path | None:
    """CSV-dump the destination table's current rows. Returns None if empty."""
    with dest.remote_engine.connect() as conn:
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    if df.empty:
        return None
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = BACKUP_DIR / f"{dest.alias}_{table}_{ts}.csv"
    df.to_csv(out, index=False)
    logger.info(f"Backed up {len(df)} rows from {dest.alias}.{table} -> {out}")
    return out


def dest_count(dest: DatabaseConfig, table: str) -> int | None:
    """Destination row count, or None if the table does not exist yet."""
    if not table_exists(dest.remote_engine, table):
        return None
    with dest.remote_engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()


def seed_table(src: DatabaseConfig, dest: DatabaseConfig, model, *, apply: bool) -> bool:
    table = model.__tablename__
    cols = list(model.__table__.columns.keys())

    print(f"\n=== {table} ({src.alias} -> {dest.alias}) ===")
    rows = read_source_rows(src, table, cols)
    before = dest_count(dest, table)
    before_str = "missing" if before is None else str(before)
    print(f"Source rows: {len(rows)} | Destination rows: {before_str}")

    if not apply:
        print(f"Plan: ensure schema, backup {before_str} dest rows, "
              f"wipe + insert {len(rows)} rows.")
        print("DRY RUN — no changes made. Pass --apply to execute.")
        return True

    backup = backup_table(dest, table)
    if backup:
        print(f"Backup written: {backup}")

    col_list = ", ".join(cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    insert_sql = text(f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})")

    # Wipe + insert + verify as one transaction; a mismatch rolls everything back.
    with dest.remote_engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table}"))
        if rows:
            conn.execute(insert_sql, rows)
        copied = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        if copied != len(rows):
            raise RuntimeError(
                f"Row count mismatch after copy: source={len(rows)}, dest={copied}"
            )

    print(f"After: {copied} rows. OK")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source", default="wcmktnewkeep",
        help="Source market DB alias to copy from (LOCAL db). Default: wcmktnewkeep.",
    )
    parser.add_argument(
        "--dest", default="wcmktbkg",
        help="Destination market DB alias to seed (REMOTE Turso db). Default: wcmktbkg.",
    )
    parser.add_argument(
        "--only", action="append", metavar="table",
        help="Restrict to specific reference tables. Repeatable. "
             f"Choices: {[m.__tablename__ for m in REFERENCE_MODELS]}",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually run the migration. Default is dry-run.",
    )
    args = parser.parse_args()

    models = REFERENCE_MODELS
    if args.only:
        wanted = set(args.only)
        models = [m for m in REFERENCE_MODELS if m.__tablename__ in wanted]
        unknown = wanted - {m.__tablename__ for m in REFERENCE_MODELS}
        if unknown:
            print(f"Unknown table(s): {sorted(unknown)}. Available: "
                  f"{[m.__tablename__ for m in REFERENCE_MODELS]}")
            return 2

    src = DatabaseConfig(args.source)
    dest = DatabaseConfig(args.dest)

    print(f"Mode: {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"Source (local): {src.alias} ({src.path})")
    print(f"Destination (remote): {dest.alias} -> {dest.turso_url}")
    print(f"Tables: {[m.__tablename__ for m in models]}")

    # Ensure destination schema once, up front (only when applying). checkfirst
    # leaves existing tables untouched and creates only what's missing.
    if args.apply:
        Base.metadata.create_all(
            dest.remote_engine,
            tables=[m.__table__ for m in models],
            checkfirst=True,
        )

    failed = []
    for model in models:
        try:
            if not seed_table(src, dest, model, apply=args.apply):
                failed.append(model.__tablename__)
        except Exception as e:
            logger.exception(f"Seeding failed for {model.__tablename__}")
            print(f"EXCEPTION on {model.__tablename__}: {e}")
            failed.append(model.__tablename__)

    if failed:
        print(f"\nFAILED: {failed}")
        return 1
    print("\nAll tables OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
