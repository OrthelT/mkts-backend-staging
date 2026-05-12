"""Integration tests for ``builder_costs.runner.run``.

The contract: ``log_buildcost_update`` must be called exactly once on the
happy path and not at all on any early-return path. ``RunResult.log_stamped``
reflects the log-write outcome and is the field downstream cron callers
inspect to surface a "data fresh, frontend probe stale" mismatch.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from mkts_backend.builder_costs import runner as runner_module
from mkts_backend.builder_costs.runner import RunResult, run


@pytest.fixture
def patched_runner():
    """Patch every external dependency of ``run()`` so each test can drive
    one branch in isolation. Yields the mock dict so tests can configure
    return values per scenario.
    """
    with (
        patch.object(runner_module, "DatabaseConfig") as mock_db_cls,
        patch.object(runner_module, "init_buildcost_tables") as mock_init,
        patch.object(runner_module, "read_build_watchlist") as mock_read_watchlist,
        patch.object(runner_module, "read_jita_prices") as mock_read_prices,
        patch.object(runner_module, "run_async_fetch_builder_costs") as mock_fetch,
        patch.object(runner_module, "upsert_builder_costs") as mock_upsert,
        patch.object(runner_module, "log_buildcost_update") as mock_log,
    ):
        # Default to the happy path; individual tests override what they need.
        mock_db_cls.return_value = MagicMock(verify_db_exists=MagicMock(return_value=True))
        mock_read_watchlist.return_value = [
            {"type_id": 34, "type_name": "Tritanium", "group_name": "Mineral", "category_id": 4},
        ]
        mock_read_prices.return_value = {34: 5.0}
        summary = MagicMock(
            attempted=1,
            failed=0,
            filtered_unbuildable=0,
            filtered_out_of_scope=0,
            records=[{"type_id": 34}],
        )
        mock_fetch.return_value = summary
        mock_upsert.return_value = 1

        yield {
            "db_cls": mock_db_cls,
            "init": mock_init,
            "read_watchlist": mock_read_watchlist,
            "read_prices": mock_read_prices,
            "fetch": mock_fetch,
            "upsert": mock_upsert,
            "log": mock_log,
        }


class TestHappyPath:
    def test_log_called_once_after_upsert(self, patched_runner):
        result = run()

        assert patched_runner["upsert"].call_count == 1
        assert patched_runner["log"].call_count == 1
        # Ordering: upsert must commit before the stamp is written.
        # Side-effect-of-side-effect ordering isn't directly assertable with
        # MagicMock; we assert call count + result.log_stamped instead.
        assert isinstance(result, RunResult)
        assert result.success is True
        assert result.log_stamped is True
        assert result.fetched == 1

    def test_log_failure_sets_success_false_but_data_already_written(
        self, patched_runner
    ):
        patched_runner["log"].side_effect = OperationalError("stamp failed", None, None)

        result = run()

        # Upsert ran (data is fresh on disk).
        assert patched_runner["upsert"].call_count == 1
        # Log was attempted and raised.
        assert patched_runner["log"].call_count == 1
        # Caller can tell the difference.
        assert result.success is False
        assert result.log_stamped is False
        assert result.fetched == 1


class TestEarlyReturnsSkipLog:
    """All four early-return paths in ``run()`` must NOT invoke the log writer.

    Stamping when nothing was actually upserted would advance the frontend's
    freshness signal without any new data behind it.
    """

    def test_db_verify_fails(self, patched_runner):
        patched_runner["db_cls"].return_value = MagicMock(
            verify_db_exists=MagicMock(return_value=False)
        )

        result = run()

        assert patched_runner["log"].call_count == 0
        assert result.success is False
        assert result.log_stamped is False

    def test_empty_watchlist(self, patched_runner):
        patched_runner["read_watchlist"].return_value = []

        result = run()

        assert patched_runner["log"].call_count == 0
        assert result.success is False
        assert result.log_stamped is False

    def test_summary_attempted_zero(self, patched_runner):
        patched_runner["fetch"].return_value = MagicMock(
            attempted=0,
            failed=0,
            filtered_unbuildable=1,
            filtered_out_of_scope=0,
            records=[],
        )

        result = run()

        # attempted==0 returns success=True (nothing to do), but still no log
        # stamp — the frontend should not see a new timestamp without new data.
        assert patched_runner["log"].call_count == 0
        assert result.success is True
        assert result.log_stamped is False

    def test_summary_records_empty_after_failures(self, patched_runner):
        patched_runner["fetch"].return_value = MagicMock(
            attempted=5,
            failed=5,
            filtered_unbuildable=0,
            filtered_out_of_scope=0,
            records=[],
        )

        result = run()

        assert patched_runner["log"].call_count == 0
        assert result.success is False
        assert result.log_stamped is False
