"""Tests for the buildcost ``updatelog`` write step.

Covers ``init_buildcost_tables`` creating the table and
``log_buildcost_update`` writing exactly one row keyed by ``table_name``.

These exist because the wcmkts_new frontend depends on
``MAX(timestamp) WHERE table_name='buildcost'`` returning a current value
after every successful builder-costs refresh. Regressions here would
silently degrade the frontend freshness probe to "always skip sync".
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text

from mkts_backend.builder_costs.repository import (
    init_buildcost_tables,
    log_buildcost_update,
)
from mkts_backend.db.build_cost_models import UpdateLog


@pytest.fixture
def buildcost_engine(tmp_path):
    """File-backed SQLite engine standing in for buildcost.db's remote engine.

    File-backed (not ``:memory:``) so ``engine.dispose()`` from the code under
    test doesn't wipe the data before assertions run.
    """
    db_path = tmp_path / "buildcost_test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    yield engine
    engine.dispose()


@pytest.fixture
def fake_db(buildcost_engine):
    """MagicMock standing in for a DatabaseConfig.

    Only ``remote_engine`` is exercised by the helpers under test; using a
    plain MagicMock avoids the full DatabaseConfig init path (Turso env vars,
    settings parsing, etc.).
    """
    db = MagicMock()
    db.remote_engine = buildcost_engine
    return db


class TestInitBuildcostTables:
    def test_creates_updatelog_table(self, fake_db, buildcost_engine):
        init_buildcost_tables(fake_db)
        with buildcost_engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name='updatelog'"
                )
            ).fetchall()
        assert rows == [("updatelog",)]

    def test_creates_all_three_tables(self, fake_db, buildcost_engine):
        init_buildcost_tables(fake_db)
        with buildcost_engine.connect() as conn:
            names = {
                row[0]
                for row in conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table'")
                ).fetchall()
            }
        assert {"build_watchlist", "builder_costs", "updatelog"} <= names

    def test_is_idempotent(self, fake_db):
        init_buildcost_tables(fake_db)
        # Second call must not raise (checkfirst=True guards each table).
        init_buildcost_tables(fake_db)


class TestLogBuildcostUpdate:
    def test_writes_single_row(self, fake_db, buildcost_engine):
        init_buildcost_tables(fake_db)
        log_buildcost_update(fake_db)

        with buildcost_engine.connect() as conn:
            rows = conn.execute(
                text("SELECT table_name, timestamp FROM updatelog")
            ).fetchall()

        assert len(rows) == 1
        assert rows[0][0] == "buildcost"

    def test_timestamp_is_recent_utc(self, fake_db, buildcost_engine):
        init_buildcost_tables(fake_db)
        before = datetime.now(timezone.utc)
        log_buildcost_update(fake_db)
        after = datetime.now(timezone.utc)

        with buildcost_engine.connect() as conn:
            ts_str = conn.execute(
                text("SELECT timestamp FROM updatelog WHERE table_name='buildcost'")
            ).scalar()

        # SQLAlchemy stores naive UTC for SQLite DateTime by default;
        # parse it and treat as UTC for comparison.
        ts = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
        assert before - timedelta(seconds=1) <= ts <= after + timedelta(seconds=1)

    def test_second_call_replaces_first_row(self, fake_db, buildcost_engine):
        """Delete-then-insert keeps exactly one row per table_name."""
        init_buildcost_tables(fake_db)
        log_buildcost_update(fake_db)
        log_buildcost_update(fake_db)
        log_buildcost_update(fake_db)

        with buildcost_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM updatelog WHERE table_name='buildcost'")
            ).scalar()

        assert count == 1

    def test_custom_table_name_isolated_from_buildcost_row(
        self, fake_db, buildcost_engine
    ):
        """Different table_name values coexist; each is independently upserted."""
        init_buildcost_tables(fake_db)
        log_buildcost_update(fake_db, table_name="buildcost")
        log_buildcost_update(fake_db, table_name="something_else")
        log_buildcost_update(fake_db, table_name="buildcost")

        with buildcost_engine.connect() as conn:
            rows = conn.execute(
                text("SELECT table_name, COUNT(*) FROM updatelog GROUP BY table_name")
            ).fetchall()

        assert dict(rows) == {"buildcost": 1, "something_else": 1}


class TestUpdateLogModel:
    def test_columns_match_wcmktprod_schema(self):
        """Buildcost UpdateLog must mirror wcmktprod UpdateLog column set so
        the frontend's freshness probe query works against either DB."""
        assert set(UpdateLog.__table__.columns.keys()) == {
            "id",
            "table_name",
            "timestamp",
        }
