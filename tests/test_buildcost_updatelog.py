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

    File-backed (not ``:memory:``) so the test storage matches production
    semantics — a real buildcost.db is a SQLite file on disk, not an
    ephemeral in-memory DB.
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
        """Upsert against UNIQUE(table_name) keeps exactly one row per table_name."""
        init_buildcost_tables(fake_db)
        log_buildcost_update(fake_db)
        log_buildcost_update(fake_db)
        log_buildcost_update(fake_db)

        with buildcost_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM updatelog WHERE table_name='buildcost'")
            ).scalar()

        assert count == 1

    def test_returns_stamped_timestamp(self, fake_db, buildcost_engine):
        init_buildcost_tables(fake_db)
        before = datetime.now(timezone.utc)
        returned = log_buildcost_update(fake_db)
        after = datetime.now(timezone.utc)

        assert isinstance(returned, datetime)
        assert returned.tzinfo is not None
        assert before <= returned <= after

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
    """Schema parity tests for the buildcost-bound ``UpdateLog``.

    Both ``UpdateLog`` classes (buildcost + wcmktprod) inherit from
    ``UpdateLogMixin``, so column shape *cannot* drift unless someone
    overrides the mixin. These tests pin the shape so an accidental override
    or a mixin edit that breaks the frontend contract gets caught immediately.
    """

    def test_column_set_matches_wcmktprod(self):
        from mkts_backend.db.models import UpdateLog as WcmktprodUpdateLog

        buildcost_cols = set(UpdateLog.__table__.columns.keys())
        wcmktprod_cols = set(WcmktprodUpdateLog.__table__.columns.keys())
        assert buildcost_cols == wcmktprod_cols == {"id", "table_name", "timestamp"}

    def test_column_types_match_wcmktprod(self):
        from mkts_backend.db.models import UpdateLog as WcmktprodUpdateLog

        for col_name in ("id", "table_name", "timestamp"):
            ours = UpdateLog.__table__.columns[col_name]
            theirs = WcmktprodUpdateLog.__table__.columns[col_name]
            assert type(ours.type) is type(theirs.type), (
                f"column {col_name!r} type drift: "
                f"buildcost={type(ours.type).__name__} "
                f"wcmktprod={type(theirs.type).__name__}"
            )

    def test_constraints_pinned(self):
        cols = UpdateLog.__table__.columns
        assert cols["id"].primary_key is True
        assert cols["id"].autoincrement is True
        assert cols["table_name"].nullable is False
        assert cols["timestamp"].nullable is False

    def test_table_name_has_unique_constraint(self):
        """Schema, not writer, enforces "one row per table_name"."""
        from sqlalchemy import UniqueConstraint

        unique_cols = {
            tuple(c.name for c in constraint.columns)
            for constraint in UpdateLog.__table__.constraints
            if isinstance(constraint, UniqueConstraint)
        }
        assert ("table_name",) in unique_cols
