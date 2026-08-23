"""Tests for CommandRegistry — command resolution, aliases, no-wrong-door."""

import pytest
from mkts_backend.cli_tools.command_registry import (
    CommandRegistry,
    CommandEntry,
    get_registry,
)


class TestCommandRegistryUnit:
    """Unit tests for the CommandRegistry class itself."""

    def test_register_and_resolve_by_name(self):
        reg = CommandRegistry()
        handler = lambda args, market: True
        reg.register("foo", handler, description="test")
        entry = reg.resolve("foo")
        assert entry is not None
        assert entry.name == "foo"
        assert entry.handler is handler

    def test_resolve_by_alias(self):
        reg = CommandRegistry()
        handler = lambda args, market: True
        reg.register("fit-check", handler, aliases=["fc"])
        assert reg.resolve("fc") is not None
        assert reg.resolve("fc").name == "fit-check"

    def test_unknown_returns_none(self):
        reg = CommandRegistry()
        assert reg.resolve("nonexistent") is None

    def test_all_names_includes_aliases(self):
        reg = CommandRegistry()
        reg.register("foo", lambda a, m: True, aliases=["f", "fo"])
        names = reg.all_names()
        assert {"foo", "f", "fo"} <= names

    def test_all_commands_no_duplicates(self):
        reg = CommandRegistry()
        reg.register("a", lambda a, m: True, aliases=["aa"])
        reg.register("b", lambda a, m: True)
        cmds = reg.all_commands()
        assert len(cmds) == 2
        assert cmds[0].name == "a"
        assert cmds[1].name == "b"

    def test_command_entry_all_names(self):
        entry = CommandEntry(
            name="fit-check",
            handler=lambda a, m: True,
            aliases=["fc"],
            description="test",
        )
        assert entry.all_names == {"fit-check", "fc"}


class TestGlobalRegistry:
    """Tests for the global registry singleton populated by _register_all."""

    def test_registry_is_singleton(self):
        r1 = get_registry()
        r2 = get_registry()
        assert r1 is r2

    def test_all_expected_commands_registered(self):
        reg = get_registry()
        expected = {
            "fit-check", "fit-update", "update-fit", "update-target",
            "assets", "equiv", "sync", "validate", "parse-items",
            "update-builder-costs",
            "esi-auth", "add_watchlist", "list-fits", "needed", "module",
        }
        registered = {e.name for e in reg.all_commands()}
        assert expected <= registered, f"Missing: {expected - registered}"

    def test_aliases_resolve(self):
        reg = get_registry()
        assert reg.resolve("fc") is not None
        assert reg.resolve("fc").name == "fit-check"
        assert reg.resolve("lf") is not None
        assert reg.resolve("lf").name == "list-fits"
        assert reg.resolve("add-watchlist") is not None
        assert reg.resolve("add-watchlist").name == "add_watchlist"
        assert reg.resolve("builder-costs") is not None
        assert reg.resolve("builder-costs").name == "update-builder-costs"

    def test_all_commands_have_handler_and_description(self):
        reg = get_registry()
        for entry in reg.all_commands():
            assert callable(entry.handler), f"{entry.name} handler not callable"
            assert entry.description, f"{entry.name} missing description"

    def test_no_wrong_door_same_commands_reachable(self):
        """Commands accessible from mkts-backend should also resolve from
        fitcheck (and vice versa) via the shared registry."""
        reg = get_registry()
        # These used to be fitcheck-only subcommands
        for name in ["list-fits", "module", "needed"]:
            assert reg.resolve(name) is not None, f"{name} not in registry"
        # These used to be mkts-backend-only commands
        for name in ["sync", "validate", "equiv", "assets"]:
            assert reg.resolve(name) is not None, f"{name} not in registry"


class TestValidateHandler:
    """The validate handler, not just its registration.

    ``validate`` called a DatabaseConfig method that the pyturso migration
    deleted, so ``mkts-backend validate`` raised AttributeError while the
    registration tests above stayed green.
    """

    def _run_validate(self, validate_sync_result: bool):
        from unittest.mock import MagicMock, patch

        db = MagicMock()
        db.alias = "wcmkttest"
        db.validate_sync.return_value = validate_sync_result
        entry = get_registry().resolve("validate")
        with patch("mkts_backend.config.db_config.DatabaseConfig", return_value=db):
            return entry.handler([], "primary"), db

    def test_reports_success_when_nothing_pending(self):
        result, db = self._run_validate(True)
        assert result is True
        db.validate_sync.assert_called_once()

    def test_reports_failure_when_changes_pending(self):
        result, db = self._run_validate(False)
        assert result is False

    def test_validate_sync_exists_on_database_config(self):
        """The handler mock cannot catch a deleted method — pin it directly."""
        from mkts_backend.config.db_config import DatabaseConfig

        assert callable(getattr(DatabaseConfig, "validate_sync", None))

