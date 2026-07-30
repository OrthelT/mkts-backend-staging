"""
Tests for module_equivalents remote sync behaviour.

Regression coverage for the case where the local table already contains a
group but the Turso remote has drifted: re-running ``equiv find --add`` must
reconcile the remote instead of silently reporting "skipped".
"""

import pytest
from sqlalchemy import create_engine, text

from mkts_backend.db import equiv_handlers


CREATE_EQUIV = """
    CREATE TABLE IF NOT EXISTS module_equivalents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        equiv_group_id INTEGER NOT NULL,
        type_id INTEGER NOT NULL,
        type_name VARCHAR(255) NOT NULL
    )
"""


class FakeDB:
    """Minimal DatabaseConfig stand-in with distinct local/remote engines."""

    def __init__(self, local_engine, remote_engine):
        self.alias = "testmkt"
        self.engine = local_engine
        self.remote_engine = remote_engine


@pytest.fixture
def equiv_env(tmp_path, monkeypatch):
    local = create_engine(f"sqlite:///{tmp_path/'local.db'}")
    remote = create_engine(f"sqlite:///{tmp_path/'remote.db'}")
    for eng in (local, remote):
        with eng.begin() as conn:
            conn.execute(text(CREATE_EQUIV))

    db = FakeDB(local, remote)
    monkeypatch.setattr(equiv_handlers, "_get_db", lambda market_ctx=None: db)
    monkeypatch.setattr(
        equiv_handlers, "resolve_type_name", lambda tid: f"Module {tid}"
    )
    return db


def _rows(engine):
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT equiv_group_id, type_id FROM module_equivalents "
                 "ORDER BY type_id")
        ).fetchall()


def test_add_equiv_group_writes_local_and_remote(equiv_env):
    gid = equiv_handlers.add_equiv_group([100, 200])
    assert gid == 1
    assert _rows(equiv_env.engine) == _rows(equiv_env.remote_engine)


def test_existing_group_still_reconciles_drifted_remote(equiv_env):
    """The regression: local has the group, remote drifted, retry must repair."""
    equiv_handlers.add_equiv_group([100, 200])

    # Simulate remote drift (failed sync, remote reset, etc.)
    with equiv_env.remote_engine.begin() as conn:
        conn.execute(text("DELETE FROM module_equivalents"))
    assert _rows(equiv_env.remote_engine) == []

    # Re-running add for the same type IDs is a local no-op...
    assert equiv_handlers.add_equiv_group([100, 200]) is None

    # ...but it must NOT leave the remote broken.
    assert _rows(equiv_env.remote_engine) == _rows(equiv_env.engine)


def test_sync_failure_is_reported_not_swallowed(equiv_env, monkeypatch):
    """A broken remote must surface as a failure, not a silent success."""
    class BrokenDB:
        alias = "testmkt"
        engine = equiv_env.engine

        @property
        def remote_engine(self):
            raise RuntimeError("Turso remote not configured")

    monkeypatch.setattr(equiv_handlers, "_get_db", lambda market_ctx=None: BrokenDB())
    assert equiv_handlers.sync_equiv_to_remote() is False
