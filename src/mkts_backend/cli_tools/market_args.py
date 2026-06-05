"""Centralized market argument parsing for CLI commands."""

import sys
from mkts_backend.config.settings_service import SettingsService

settings_service = SettingsService()

MARKET_SYNONYMS: dict[str, str] = {
    "north": "deployment",
}

# Every configured market, derived from the [markets.*] sections themselves
# (settings.toml order → deterministic meta-alias expansion). Single source of
# truth shared with database_routing/MarketContext — no parallel alias list to
# drift. The set form is for fast membership checks.
_ALL_MARKETS: list[str] = settings_service.market_aliases
VALID_MARKET_ALIASES: set[str] = set(_ALL_MARKETS)
MARKET_DB_MAP = {k: settings_service.markets_raw[k]["database_alias"] for k in _ALL_MARKETS}

# "all" is the canonical alias for "every configured market". "both" is a legacy
# synonym kept only so old scripts/flags don't break; everything user-facing says "all".
ALL_MARKETS_ALIAS = "all"
_META_ALIASES: set[str] = {"all", "both"}

_UNSPECIFIED = "__unspecified__"
DEFAULT_MARKET_ALIAS = settings_service.default_market_alias


def expand_market_alias(alias: str) -> list[str]:
    """Expand a market alias into the list of concrete markets to act on.

    ``"all"`` (or legacy ``"both"``) → every configured market
    (``["primary", "deployment", "market3"]``); anything else → ``[alias]``.
    """
    return list(_ALL_MARKETS) if alias in _META_ALIASES else [alias]


def resolve_market_alias(args: list[str]) -> str | None:
    """Return the explicit market alias if the user specified one, else ``None``.

    Distinguishes "user gave no flag" from "user explicitly picked --market=<alias>",
    which the subcommand-default logic in ``parse_args`` needs.
    """
    resolved = parse_market_args(args, default=_UNSPECIFIED)
    return None if resolved == _UNSPECIFIED else resolved


def resolve_market_alias_interactive(default: str = DEFAULT_MARKET_ALIAS) -> str:
    """Prompt the user to pick a market alias when the current choice is ambiguous.

    Returns one of ``primary`` / ``deployment`` / ``all``. In non-TTY
    sessions returns ``default`` without prompting so scripts keep working.
    """
    if not sys.stdin.isatty():
        return default
    from rich.console import Console
    from rich.prompt import Prompt

    console = Console()
    console.print("\n[yellow]This command needs a specific market — pick one:[/yellow]")
    console.print("  1) primary")
    console.print("  2) deployment")
    console.print("  3) all")
    default_choice = {"primary": "1", "deployment": "2", "all": "3"}.get(default, "1")
    choice = Prompt.ask("Choice", choices=["1", "2", "3"], default=default_choice)
    return {"1": "primary", "2": "deployment", "3": ALL_MARKETS_ALIAS}[choice]


def parse_market_args(args: list[str], default: str = "primary") -> str:
    """Scan args for market flags and return a normalized market alias.

    Recognizes --market=<value>, --deployment, --north, --primary, --all,
    and bare positional aliases (e.g. ``deployment`` without ``--``).
    Returns 'primary', 'deployment', 'market3', or 'all'. The legacy ``--both``
    flag and ``--market=both`` are accepted and normalized to ``all``.
    """
    # First pass: explicit --flags take priority
    for arg in args:
        if arg.startswith("--market="):
            val = arg.split("=", 1)[1].lower()
            resolved = MARKET_SYNONYMS.get(val, val)
            if resolved in _META_ALIASES:
                return ALL_MARKETS_ALIAS
            if resolved not in VALID_MARKET_ALIASES:
                print(f"Error: unknown market '{val}'. Valid options: {', '.join(sorted(VALID_MARKET_ALIASES))}, all")
                sys.exit(1)
            return resolved
        if arg in ("--deployment", "--north"):
            return "deployment"
        if arg == "--primary":
            return "primary"
        if arg in ("--all", "--both"):
            return ALL_MARKETS_ALIAS
    # Second pass: bare positional aliases (e.g. ``mkts-backend deployment ...``)
    for arg in args:
        if arg.startswith("-"):
            continue
        resolved = MARKET_SYNONYMS.get(arg, arg)
        if resolved in _META_ALIASES:
            return ALL_MARKETS_ALIAS
        if resolved in VALID_MARKET_ALIASES:
            return resolved
    return default
