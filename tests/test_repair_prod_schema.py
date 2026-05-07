"""Tests for the prod schema repair migration.

The migration drops/recreates production tables with PK constraints
restored, copying rows via COALESCE to fill NULLs left over from the
pandas-replace era. These tests pin the contract: PK after, row count
preserved, NULLs filled, idempotent on already-correct tables, and
refusing on duplicate type_ids.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from mkts_backend.db.models import Base, Watchlist, MarketStats


def _load_repair_module():
    """Load scripts/repair_prod_schema.py as a module without scripts being a package."""
    src = Path(__file__).resolve().parent.parent / "scripts" / "repair_prod_schema.py"
    spec = importlib.util.spec_from_file_location("repair_prod_schema", src)
    module = importlib.util.module_from_spec(spec)
    sys.modules["repair_prod_schema"] = module
    spec.loader.exec_module(module)
    return module


repair = _load_repair_module()


class _FakeDB:
    def __init__(self, engine, alias: str = "fake"):
        self.engine = engine
        self.remote_engine = engine
        self.alias = alias


def _make_pandas_shape_watchlist(engine, rows: list[dict]) -> None:
    """Recreate the broken (no-PK, pandas-shaped) watchlist that the
    migration is designed to repair.
    """
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS watchlist"))
        conn.execute(text(
            "CREATE TABLE watchlist ("
            "type_id BIGINT, type_name TEXT, "
            "group_id BIGINT, group_name TEXT, "
            "category_id BIGINT, category_name TEXT)"
        ))
        for row in rows:
            conn.execute(
                text(
                    "INSERT INTO watchlist (type_id, type_name, group_id, "
                    "group_name, category_id, category_name) "
                    "VALUES (:type_id, :type_name, :group_id, :group_name, "
                    ":category_id, :category_name)"
                ),
                row,
            )


@pytest.fixture
def fake_db(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path}/repair.db")
    monkeypatch.setattr(repair, "BACKUP_DIR", tmp_path / "backups")
    yield _FakeDB(engine, alias="testdb")
    engine.dispose()


# ---- build_select_expr returns a tuple --------------------------------------


def test_build_select_expr_returns_tuple_of_str_and_columns():
    expr, cols = repair.build_select_expr("watchlist", "src")
    assert isinstance(expr, str)
    assert isinstance(cols, list)
    assert "type_id" in cols
    assert "COALESCE(src.type_id" in expr


# ---- migrate_target end-to-end ----------------------------------------------


def test_migrate_target_restores_pk_and_preserves_rows(fake_db):
    _make_pandas_shape_watchlist(fake_db.engine, [
        {"type_id": 1, "type_name": "a", "group_id": 10,
         "group_name": "g", "category_id": 100, "category_name": "c"},
        {"type_id": 2, "type_name": "b", "group_id": 20,
         "group_name": "g", "category_id": 200, "category_name": "c"},
    ])

    ok = repair.migrate_target(fake_db, "watchlist", Watchlist, apply=True)

    assert ok is True
    pk_after = repair.get_pk_cols(fake_db, "watchlist")
    assert pk_after == ["type_id"]
    with fake_db.engine.connect() as conn:
        ids = conn.execute(text("SELECT type_id FROM watchlist ORDER BY type_id")).fetchall()
    assert [r.type_id for r in ids] == [1, 2]


def test_migrate_target_is_idempotent_on_already_repaired_table(fake_db):
    """Calling migrate_target on a table that already has the correct PK
    must short-circuit and not touch the data.
    """
    Base.metadata.tables["watchlist"].create(fake_db.engine)
    with fake_db.engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO watchlist (type_id, type_name, group_id, group_name, "
            "category_id, category_name) "
            "VALUES (1, 'a', 10, 'g', 100, 'c')"
        ))

    ok = repair.migrate_target(fake_db, "watchlist", Watchlist, apply=True)

    assert ok is True
    with fake_db.engine.connect() as conn:
        rows = conn.execute(text("SELECT type_id, type_name FROM watchlist")).fetchall()
    assert [(r.type_id, r.type_name) for r in rows] == [(1, "a")]


def test_migrate_target_refuses_when_source_has_duplicate_type_ids(fake_db):
    """If the broken table contains duplicate type_ids, the migration
    must refuse rather than crash on PK collision mid-copy.
    """
    _make_pandas_shape_watchlist(fake_db.engine, [
        {"type_id": 1, "type_name": "a", "group_id": 10,
         "group_name": "g", "category_id": 100, "category_name": "c"},
        {"type_id": 1, "type_name": "duplicate", "group_id": 10,
         "group_name": "g", "category_id": 100, "category_name": "c"},
    ])

    ok = repair.migrate_target(fake_db, "watchlist", Watchlist, apply=True)

    assert ok is False
    pk_after = repair.get_pk_cols(fake_db, "watchlist")
    assert pk_after == [], "table must be unchanged when refused"


def test_migrate_target_dry_run_does_not_write(fake_db):
    """apply=False prints the plan and returns True, but the broken
    schema must remain unchanged.
    """
    _make_pandas_shape_watchlist(fake_db.engine, [
        {"type_id": 1, "type_name": "a", "group_id": 10,
         "group_name": "g", "category_id": 100, "category_name": "c"},
    ])

    ok = repair.migrate_target(fake_db, "watchlist", Watchlist, apply=False)

    assert ok is True
    pk_after = repair.get_pk_cols(fake_db, "watchlist")
    assert pk_after == [], "dry-run must not modify the schema"


def test_migrate_target_fills_nulls_via_coalesce(fake_db):
    """A NULL in a source column that maps to a NOT NULL Base column
    must be replaced with the documented DEFAULTS value, not abort the
    migration.
    """
    with fake_db.engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS watchlist"))
        conn.execute(text(
            "CREATE TABLE watchlist ("
            "type_id BIGINT, type_name TEXT, "
            "group_id BIGINT, group_name TEXT, "
            "category_id BIGINT, category_name TEXT)"
        ))
        conn.execute(text(
            "INSERT INTO watchlist (type_id, type_name, group_id, group_name, "
            "category_id, category_name) "
            "VALUES (1, NULL, 10, NULL, 100, NULL)"
        ))

    ok = repair.migrate_target(fake_db, "watchlist", Watchlist, apply=True)

    assert ok is True
    with fake_db.engine.connect() as conn:
        row = conn.execute(text(
            "SELECT type_id, type_name, group_name, category_name FROM watchlist"
        )).fetchone()
    assert row.type_id == 1
    assert row.type_name == ""
    assert row.group_name == ""
    assert row.category_name == ""


def test_migrate_target_preserves_pk_for_marketstats(fake_db):
    """Same regression coverage as watchlist, but for the marketstats
    table — it has more columns and is also in the migration TARGETS list.
    """
    with fake_db.engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS marketstats"))
        conn.execute(text(
            "CREATE TABLE marketstats ("
            "type_id BIGINT, total_volume_remain BIGINT, "
            "min_price REAL, price REAL, avg_price REAL, avg_volume REAL, "
            "group_id BIGINT, type_name TEXT, group_name TEXT, "
            "category_id BIGINT, category_name TEXT, "
            "days_remaining REAL, last_update TEXT)"
        ))
        conn.execute(text(
            "INSERT INTO marketstats VALUES "
            "(1, 100, 1.0, 2.0, 1.5, 50.0, 10, 'item', 'g', "
            "100, 'c', 30.0, '2024-01-01 00:00:00')"
        ))

    ok = repair.migrate_target(fake_db, "marketstats", MarketStats, apply=True)

    assert ok is True
    pk_after = repair.get_pk_cols(fake_db, "marketstats")
    assert pk_after == ["type_id"]
    with fake_db.engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM marketstats")).scalar()
    assert count == 1
