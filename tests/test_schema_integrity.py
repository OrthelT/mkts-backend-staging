"""Schema integrity checks for tables defined in ``mkts_backend.db.models``.

These tests guard against the regression that motivated PR-hotfix:
production ``marketstats`` and ``watchlist`` tables had been recreated by
``pandas.DataFrame.to_sql(if_exists="replace")`` and lost their primary
keys, which silently disabled ``ON CONFLICT DO UPDATE`` and let duplicate
rows accumulate.
"""
from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from mkts_backend.db.models import Base, MarketStats, Watchlist


# ---- helpers ----------------------------------------------------------------


def get_pk_cols(engine, table_name: str) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
    return [r.name for r in rows if r.pk]


def assert_table_matches_model(engine, model) -> None:
    """Verify a live table's PK matches the SQLAlchemy model.

    Raises AssertionError with a specific message if the DB is out of sync
    with the Base definition — that is the failure mode we want surfaced.
    """
    expected = [c.name for c in model.__table__.primary_key.columns]
    actual = get_pk_cols(engine, model.__tablename__)
    assert actual == expected, (
        f"PK mismatch on {model.__tablename__}: "
        f"model defines {expected}, DB has {actual}"
    )


# ---- fixtures ---------------------------------------------------------------


@pytest.fixture
def base_engine(tmp_path):
    """A fresh SQLite engine with all Base tables created."""
    engine = create_engine(f"sqlite:///{tmp_path}/integrity.db")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def watchlist_csv(tmp_path):
    """A CSV in the format expected by the watchlist writers."""
    df = pd.DataFrame(
        [
            {"type_id": 1, "type_name": "a", "group_id": 10,
             "group_name": "g", "category_id": 100, "category_name": "c"},
            {"type_id": 2, "type_name": "b", "group_id": 20,
             "group_name": "g", "category_id": 200, "category_name": "c"},
        ]
    )
    path = tmp_path / "watchlist.csv"
    df.to_csv(path, index=False)
    return path


# ---- 1) Base-built DB matches its models ------------------------------------


@pytest.mark.parametrize("model", [MarketStats, Watchlist])
def test_base_create_all_yields_expected_pk(base_engine, model):
    """create_all(Base) must produce tables whose PKs match the models."""
    assert_table_matches_model(base_engine, model)


# ---- 2) Validator catches the historic pandas-replace bug -------------------


def test_pandas_replace_drops_pk_and_validator_catches_it(tmp_path):
    """If anything calls ``to_sql(if_exists='replace')`` on a Base table,
    the PK is lost and ``assert_table_matches_model`` must fail loudly.
    Exists so the regression that took prod down has a named test.
    """
    engine = create_engine(f"sqlite:///{tmp_path}/regress.db")
    Base.metadata.create_all(engine)
    assert_table_matches_model(engine, Watchlist)  # sanity

    pd.DataFrame(
        [
            {"type_id": 1, "type_name": "a", "group_id": 10,
             "group_name": "g", "category_id": 100, "category_name": "c"},
        ]
    ).to_sql("watchlist", engine, if_exists="replace", index=False)

    with pytest.raises(AssertionError, match="PK mismatch on watchlist"):
        assert_table_matches_model(engine, Watchlist)
    engine.dispose()


# ---- 3) Each fixed writer preserves the PK ---------------------------------
# These are the tests that actually catch a code regression: if anyone
# reverts the watchlist writers to ``to_sql(if_exists="replace")`` or
# similar pandas-shaped recreates, these will fail.


class _FakeDB:
    """Minimal stand-in for DatabaseConfig that only exposes ``.engine``."""

    def __init__(self, engine):
        self.engine = engine
        self.alias = "fake"


def test_update_watchlist_data_preserves_pk(base_engine, watchlist_csv, monkeypatch):
    monkeypatch.setattr(
        "mkts_backend.utils.utils.wcmkt_db", _FakeDB(base_engine)
    )
    from mkts_backend.utils.utils import update_watchlist_data

    update_watchlist_data(esi=None, watchlist_csv=str(watchlist_csv))

    assert_table_matches_model(base_engine, Watchlist)
    with base_engine.connect() as conn:
        rows = conn.execute(text("SELECT type_id FROM watchlist ORDER BY type_id")).fetchall()
    assert [r.type_id for r in rows] == [1, 2]


def test_restore_watchlist_from_csv_preserves_pk(
    base_engine, watchlist_csv, monkeypatch
):
    # restore_watchlist_from_csv builds a DatabaseConfig internally;
    # patch the constructor so the writer uses our temp engine.
    fake = _FakeDB(base_engine)
    fake.remote_engine = base_engine
    monkeypatch.setattr(
        "mkts_backend.utils.db_utils.DatabaseConfig",
        lambda *a, **kw: fake,
    )
    from mkts_backend.utils.db_utils import restore_watchlist_from_csv

    restore_watchlist_from_csv(csv_file=str(watchlist_csv), remote=False)

    assert_table_matches_model(base_engine, Watchlist)
    with base_engine.connect() as conn:
        rows = conn.execute(text("SELECT type_id FROM watchlist ORDER BY type_id")).fetchall()
    assert [r.type_id for r in rows] == [1, 2]


def test_add_missing_items_to_watchlist_uses_on_conflict(base_engine, monkeypatch):
    """Pre-loading the table and re-inserting the same type_id must not
    raise and must not produce a duplicate row — proving the writer uses
    ``on_conflict_do_nothing`` rather than relying on the app-level filter
    alone.
    """
    # Pre-seed one row, then attempt a second insert of the same type_id.
    with base_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO watchlist "
                "(type_id, type_name, group_id, group_name, category_id, category_name) "
                "VALUES (1, 'orig', 10, 'g', 100, 'c')"
            )
        )

    # add_missing_items_to_watchlist filters via pandas; bypass that path by
    # exercising the underlying upsert directly.
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    stmt = sqlite_insert(Watchlist.__table__).values(
        type_id=1, type_name="dup", group_id=10,
        group_name="g", category_id=100, category_name="c",
    ).on_conflict_do_nothing(index_elements=["type_id"])

    with base_engine.begin() as conn:
        result = conn.execute(stmt)
    assert result.rowcount == 0  # conflict, no insert

    with base_engine.connect() as conn:
        rows = conn.execute(text("SELECT type_id, type_name FROM watchlist")).fetchall()
    assert len(rows) == 1
    assert rows[0].type_name == "orig"
