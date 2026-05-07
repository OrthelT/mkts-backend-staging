"""Repair production schema by replacing pandas-shaped tables with Base-shaped ones.

Targets (cloud Turso DBs):
  - wcmktprod.marketstats  (no PK, BIGINT types)
  - wcmktprod.watchlist    (no PK, BIGINT types)
  - wcmktnorth.watchlist   (no PK, INT types)

Each target is migrated by:
  1. CSV backup of current rows
  2. CREATE TABLE <t>_new (Base schema)
  3. INSERT INTO <t>_new SELECT ... FROM <t> (with COALESCE for NOT NULL)
  4. DROP TABLE <t>; ALTER TABLE <t>_new RENAME TO <t>
  5. Verify row count and PK presence

Dry-run by default. Pass --apply to actually write.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.schema import CreateTable

from mkts_backend.config.db_config import DatabaseConfig
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.db.models import MarketStats, Watchlist, Base

logger = configure_logging(__name__)

BACKUP_DIR = Path("data/migration_backups")


# (alias, table_name, model)
TARGETS = [
    ("wcmktprod", "marketstats", MarketStats),
    ("wcmktprod", "watchlist", Watchlist),
    ("wcmktnorth", "watchlist", Watchlist),
]


# Per-table COALESCE expressions to fill any nulls when copying from the old
# pandas-shaped table into the new NOT NULL Base-shaped table.
DEFAULTS = {
    "marketstats": {
        "type_id": "0",
        "total_volume_remain": "0",
        "min_price": "0.0",
        "price": "0.0",
        "avg_price": "0.0",
        "avg_volume": "0.0",
        "group_id": "0",
        "type_name": "''",
        "group_name": "''",
        "category_id": "0",
        "category_name": "''",
        "days_remaining": "0.0",
        "last_update": "CURRENT_TIMESTAMP",
    },
    "watchlist": {
        "type_id": "0",
        "group_id": "0",
        "type_name": "''",
        "group_name": "''",
        "category_id": "0",
        "category_name": "''",
    },
}


def backup_table(db: DatabaseConfig, table: str) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = BACKUP_DIR / f"{db.alias}_{table}_{ts}.csv"
    with db.remote_engine.connect() as conn:
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    df.to_csv(out, index=False)
    logger.info(f"Backed up {len(df)} rows from {db.alias}.{table} -> {out}")
    return out


def get_pk_cols(db: DatabaseConfig, table: str, remote: bool = True) -> list[str]:
    engine = db.remote_engine if remote else db.engine
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    return [r.name for r in rows if r.pk]


def row_count(db: DatabaseConfig, table: str) -> int:
    with db.remote_engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()


def duplicate_type_ids(db: DatabaseConfig, table: str) -> list[tuple]:
    with db.remote_engine.connect() as conn:
        return conn.execute(
            text(
                f"SELECT type_id, COUNT(*) c FROM {table} "
                f"GROUP BY type_id HAVING c>1"
            )
        ).fetchall()


def build_select_expr(table: str, source: str) -> str:
    """Build a SELECT list with COALESCE for each Base column."""
    defaults = DEFAULTS[table]
    cols = list(defaults.keys())
    pieces = [f"COALESCE({source}.{c}, {defaults[c]}) AS {c}" for c in cols]
    return ", ".join(pieces), cols


def migrate_target(db: DatabaseConfig, table: str, model, *, apply: bool) -> bool:
    print(f"\n=== {db.alias}.{table} ===")
    pk_before = get_pk_cols(db, table)
    rows_before = row_count(db, table)
    dups_before = duplicate_type_ids(db, table)
    print(f"Before: rows={rows_before}, pk={pk_before}, duplicates={len(dups_before)}")

    if pk_before == ["type_id"]:
        print("Already has PK on type_id — nothing to do.")
        return True

    if dups_before:
        print(f"REFUSING: {len(dups_before)} duplicate type_ids in source table.")
        print(f"   {dups_before[:5]}{' …' if len(dups_before) > 5 else ''}")
        return False

    backup = backup_table(db, table)
    print(f"Backup written: {backup}")

    select_expr, cols = build_select_expr(table, "src")
    column_list = ", ".join(cols)
    new_table = f"{table}_new"

    # Build CREATE TABLE statement from the SQLAlchemy model. Use sqlite dialect
    # since libsql speaks SQLite. Substitute the table name to <t>_new.
    ddl = str(
        CreateTable(model.__table__).compile(
            dialect=db.remote_engine.dialect
        )
    ).strip().rstrip(";")
    ddl = ddl.replace(f"CREATE TABLE {table}", f"CREATE TABLE {new_table}", 1)

    print(f"\nDDL for new table:\n{ddl}")
    print(f"\nINSERT plan:\n  INSERT INTO {new_table} ({column_list})")
    print(f"  SELECT {select_expr} FROM {table} src;")

    if not apply:
        print("\nDRY RUN — no changes made. Pass --apply to execute.")
        return True

    # Execute migration as a single transaction.
    engine = db.remote_engine
    with engine.begin() as conn:
        # Defensive: drop any leftover *_new from a previous failed run.
        conn.execute(text(f"DROP TABLE IF EXISTS {new_table}"))
        conn.execute(text(ddl))
        conn.execute(
            text(
                f"INSERT INTO {new_table} ({column_list}) "
                f"SELECT {select_expr} FROM {table} src"
            )
        )
        copied = conn.execute(
            text(f"SELECT COUNT(*) FROM {new_table}")
        ).scalar()
        if copied != rows_before:
            raise RuntimeError(
                f"Row count mismatch after copy: src={rows_before}, new={copied}"
            )
        conn.execute(text(f"DROP TABLE {table}"))
        conn.execute(text(f"ALTER TABLE {new_table} RENAME TO {table}"))

    pk_after = get_pk_cols(db, table)
    rows_after = row_count(db, table)
    print(f"After:  rows={rows_after}, pk={pk_after}")

    if pk_after != ["type_id"]:
        print(f"FAIL: PK not present after migration. Got {pk_after}")
        return False
    if rows_after != rows_before:
        print(f"FAIL: row count drift: before={rows_before}, after={rows_after}")
        return False
    print("OK")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually run the migration. Default is dry-run.",
    )
    parser.add_argument(
        "--only",
        action="append",
        metavar="alias.table",
        help="Restrict to a single target, e.g. --only wcmktprod.watchlist. "
        "Repeatable.",
    )
    args = parser.parse_args()

    targets = TARGETS
    if args.only:
        wanted = set(args.only)
        targets = [t for t in TARGETS if f"{t[0]}.{t[1]}" in wanted]
        if not targets:
            print(f"No matching targets for {args.only}. Available: "
                  f"{[f'{a}.{t}' for a, t, _ in TARGETS]}")
            return 2

    print(f"Mode: {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"Targets: {[f'{a}.{t}' for a, t, _ in targets]}")

    failed = []
    for alias, table, model in targets:
        db = DatabaseConfig(alias)
        try:
            ok = migrate_target(db, table, model, apply=args.apply)
            if not ok:
                failed.append(f"{alias}.{table}")
        except Exception as e:
            logger.exception(f"Migration failed for {alias}.{table}")
            print(f"EXCEPTION on {alias}.{table}: {e}")
            failed.append(f"{alias}.{table}")

    if failed:
        print(f"\nFAILED: {failed}")
        return 1
    print("\nAll targets OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
