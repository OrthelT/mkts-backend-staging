"""
Tests for MarketContext routing logic and error handling.

Intentionally minimal. These assert behavior that catches real regressions
(the dev-mode DB override, deployment env-independence, unknown-alias errors)
and are expressed *relationally*, so editing market values in settings.toml
never requires touching them. The former value-echo tests — asserting that a
structure_id / region_id / database alias equals the literal already written in
settings.toml — were deleted: they only ever broke on config edits and never
caught a bug.
"""
import pytest


def test_invalid_market_alias_raises_error():
    """Unknown market alias raises ValueError."""
    from mkts_backend.config.market_context import MarketContext

    with pytest.raises(ValueError, match="Unknown market"):
        MarketContext.from_settings("invalid_market")


def test_get_available_markets_lists_configured_markets():
    """get_available_markets() returns the configured market aliases."""
    from mkts_backend.config.market_context import MarketContext

    markets = MarketContext.get_available_markets()

    assert "primary" in markets
    assert "deployment" in markets


def test_primary_dev_override_routes_to_different_db_than_prod(monkeypatch):
    """Regression guard: in development the primary market must route to a
    *different* database than in production (the dev override fires). The actual
    alias is irrelevant — only that the override changes it. Guards the bug
    where ``--env=development`` silently routed primary to the production DB.
    """
    from mkts_backend.config.market_context import MarketContext
    from mkts_backend.config.settings_service import clear_cache

    monkeypatch.setenv("MKTS_ENVIRONMENT", "development")
    clear_cache()
    dev = MarketContext.from_settings("primary")

    monkeypatch.setenv("MKTS_ENVIRONMENT", "production")
    clear_cache()
    prod = MarketContext.from_settings("primary")
    clear_cache()

    assert dev.database_alias != prod.database_alias


def test_deployment_is_env_independent(monkeypatch):
    """Regression guard: deployment routes to the same DB in dev and prod —
    the override applies only to primary."""
    from mkts_backend.config.market_context import MarketContext
    from mkts_backend.config.settings_service import clear_cache

    monkeypatch.setenv("MKTS_ENVIRONMENT", "development")
    clear_cache()
    dev = MarketContext.from_settings("deployment")

    monkeypatch.setenv("MKTS_ENVIRONMENT", "production")
    clear_cache()
    prod = MarketContext.from_settings("deployment")
    clear_cache()

    assert dev.database_alias == prod.database_alias
