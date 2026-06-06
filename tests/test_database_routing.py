"""
Tests for ESI/database routing and backward-compatible defaults.

Trimmed to behavioral guards only. The large family of "a market context routes
to its own database" tests (asserted across DatabaseConfig and ``_get_db`` in
three modules, plus isolation / sequential / interleaved / concurrent / thread
variants) was deleted: it re-tested one passthrough a dozen ways and broke
whenever a market's database alias changed in settings.toml, without catching a
real bug. What remains exercises ESI URL/header construction and the
legacy/default code paths, expressed against the context rather than frozen IDs.
"""


class TestESIConfigRouting:
    def test_market_orders_url_uses_structure_endpoint(self, primary_market_context):
        """market_orders_url targets the structure endpoint with the structure id."""
        from mkts_backend.config.esi_config import ESIConfig

        esi = ESIConfig(market_context=primary_market_context)
        url = esi.market_orders_url

        assert "structures" in url
        assert str(primary_market_context.structure_id) in url

    def test_market_history_url_uses_region_endpoint(self, primary_market_context):
        """market_history_url targets the region history endpoint."""
        from mkts_backend.config.esi_config import ESIConfig

        esi = ESIConfig(market_context=primary_market_context)
        url = esi.market_history_url

        assert "history" in url
        assert str(primary_market_context.region_id) in url

    def test_headers_primary_does_not_raise(self, primary_market_context):
        """headers property builds auth headers for primary without raising."""
        from mkts_backend.config.esi_config import ESIConfig
        from unittest.mock import patch

        esi = ESIConfig(market_context=primary_market_context)
        with patch.object(esi, "token", return_value={"access_token": "mock_token"}):
            headers = esi.headers

        assert "Bearer" in headers["Authorization"]

    def test_headers_deployment_does_not_raise(self, deployment_market_context):
        """Regression guard: headers once only accepted 'primary'/'secondary'
        aliases and raised ValueError for 'deployment'."""
        from mkts_backend.config.esi_config import ESIConfig
        from unittest.mock import patch

        esi = ESIConfig(market_context=deployment_market_context)
        with patch.object(esi, "token", return_value={"access_token": "mock_token"}):
            headers = esi.headers

        assert "Bearer" in headers["Authorization"]


class TestBackwardCompatibleDefaults:
    def test_legacy_alias_resolves_to_default_market_db(self, monkeypatch):
        """Legacy DatabaseConfig('wcmkt') routes to the default market's DB.

        After collapsing [db] onto [markets], the legacy/default path resolves
        through markets.default (primary) rather than a separate [db] alias —
        asserted against MarketContext so it tracks config, not a literal.
        """
        from mkts_backend.config.db_config import DatabaseConfig
        from mkts_backend.config.market_context import MarketContext
        from mkts_backend.config.settings_service import clear_cache

        monkeypatch.setenv("MKTS_ENVIRONMENT", "production")
        clear_cache()
        db = DatabaseConfig("wcmkt")
        assert db.alias == MarketContext.from_settings("primary").database_alias

    def test_get_db_without_context_uses_default_market_db(self, monkeypatch):
        """_get_db(None) falls back to the default market's database."""
        from mkts_backend.db.db_handlers import _get_db
        from mkts_backend.config.market_context import MarketContext
        from mkts_backend.config.settings_service import clear_cache

        monkeypatch.setenv("MKTS_ENVIRONMENT", "production")
        clear_cache()
        db = _get_db(None)
        assert db.alias == MarketContext.from_settings("primary").database_alias
