"""CLI dispatch for ``build-watchlist [add|remove|mirror|sync]``.

Verbs:
    add    — write items to build_watchlist after SDE lookup
    remove — delete items from build_watchlist
    mirror — pull missing items from wcmktprod.watchlist (reconciliation)
    sync   — refresh local buildcost mirror from remote (db.sync())

``sync`` is the standard libsql remote→local pull. ``mirror`` is the
wcmktprod-into-buildcost reconciliation. They are deliberately separate
verbs so they don't collide semantically with the rest of the CLI, where
``sync`` always means ``DatabaseConfig.sync()``.
"""

from __future__ import annotations

import csv

from mkts_backend.builder_costs.watchlist_sync import (
    AddResult,
    RemoveResult,
    SyncResult,
    add_to_build_watchlist,
    remove_from_build_watchlist,
    sync_from_market,
)
from mkts_backend.cli_tools.arg_utils import ParsedArgs
from mkts_backend.cli_tools.prompter import get_multiline_input
from mkts_backend.config.db_config import DatabaseConfig
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.utils.get_type_info import get_type_from_list

logger = configure_logging(__name__)

_USAGE = (
    "Usage:\n"
    "  mkts-backend build-watchlist add    --type_id=12345,67890 [--force]\n"
    "  mkts-backend build-watchlist add    --file=items.csv      [--force]\n"
    "  mkts-backend build-watchlist add    --paste                [--force]\n"
    "  mkts-backend build-watchlist remove --type_id=12345,67890\n"
    "  mkts-backend build-watchlist remove --file=items.csv\n"
    "  mkts-backend build-watchlist remove --paste\n"
    "  mkts-backend build-watchlist mirror   (pull missing buildable items from wcmktprod.watchlist)\n"
    "  mkts-backend build-watchlist sync     (pull buildcost.db local mirror from remote)\n"
)


def handle_build_watchlist(args: list[str]) -> bool:
    p = ParsedArgs(args)

    subcommand = next(
        (a for a in p.positionals() if a != "build-watchlist"),
        None,
    )

    if p.has_help() and not subcommand:
        print(_USAGE)
        return True

    if not subcommand:
        print("Error: build-watchlist requires a subcommand (add | remove | mirror | sync)")
        print(_USAGE)
        return False

    handlers = {
        "add": _handle_add,
        "remove": _handle_remove,
        "mirror": _handle_mirror,
        "sync": _handle_sync,
    }
    handler = handlers.get(subcommand)
    if handler is None:
        suggestion = _suggest(subcommand, handlers.keys())
        print(f"Error: unknown subcommand '{subcommand}'.{suggestion}")
        print(_USAGE)
        return False

    return handler(p)


def _handle_add(p: ParsedArgs) -> bool:
    if p.has_help():
        print(
            "build-watchlist add: add items to build_watchlist after looking "
            "up SDE metadata.\n\n"
            "By default, items without a manufacturing blueprint in the SDE "
            "are skipped. Pass --force to add them anyway (EverRef will likely "
            "reject them, but the row will be present).\n"
            "Local buildcost mirror is auto-synced after the write; pass "
            "--no-sync to skip."
        )
        print(_USAGE)
        return True

    type_ids = _resolve_type_ids(p)
    if type_ids is None:
        return False

    force = p.has_flag("force")
    buildcost_db = DatabaseConfig("buildcost")
    sde_db = DatabaseConfig("sde")
    result = add_to_build_watchlist(buildcost_db, sde_db, type_ids, force=force)
    _print_add_summary(result, force=force)
    if result.added > 0 and not p.has_flag("no-sync"):
        _sync_buildcost_mirror(buildcost_db)
    return True


def _handle_remove(p: ParsedArgs) -> bool:
    if p.has_help():
        print(
            "build-watchlist remove: delete items from build_watchlist.\n"
            "Idempotent — type_ids that aren't present are reported but not "
            "treated as errors.\n"
            "Local buildcost mirror is auto-synced after the write; pass "
            "--no-sync to skip."
        )
        print(_USAGE)
        return True

    type_ids = _resolve_type_ids(p)
    if type_ids is None:
        return False

    buildcost_db = DatabaseConfig("buildcost")
    result = remove_from_build_watchlist(buildcost_db, type_ids)
    _print_remove_summary(result)
    if result.removed > 0 and not p.has_flag("no-sync"):
        _sync_buildcost_mirror(buildcost_db)
    return True


def _sync_buildcost_mirror(buildcost_db: DatabaseConfig) -> None:
    """Pull the buildcost local mirror after a remote write. Best-effort."""
    try:
        buildcost_db.sync()
        print("Synced local buildcost mirror")
    except Exception as exc:
        logger.warning(f"buildcost local sync failed (remote write succeeded): {exc}")
        print(f"Warning: local buildcost sync failed: {exc}")


def _handle_mirror(p: ParsedArgs) -> bool:
    if p.has_help():
        print(
            "build-watchlist mirror: reconcile build_watchlist against the "
            "primary market watchlist.\n"
            "Adds buildable items from wcmktprod.watchlist that aren't yet "
            "in build_watchlist. Never removes anything."
        )
        return True

    if (
        p.get_string("type_id", "type-id")
        or p.get_string("file")
        or p.has_flag("paste")
    ):
        print("Error: 'mirror' takes no item flags; it reads from the primary market.")
        return False

    buildcost_db = DatabaseConfig("buildcost")
    sde_db = DatabaseConfig("sde")
    primary_db = DatabaseConfig("primary")

    for db in (buildcost_db, primary_db):
        try:
            db.sync()
        except Exception as exc:
            logger.warning(f"Pre-sync of {db.alias} failed: {exc}")

    result = sync_from_market(buildcost_db, sde_db, primary_db)
    _print_mirror_summary(result)
    if result.added > 0 and not p.has_flag("no-sync"):
        _sync_buildcost_mirror(buildcost_db)
    return True


def _handle_sync(p: ParsedArgs) -> bool:
    """Pull the local buildcost mirror from the remote (libsql sync).

    No reconciliation, no remote writes — just ``DatabaseConfig.sync()``.
    Use this when another process (or another machine) has written to the
    buildcost remote and the local mirror needs to catch up.
    """
    if p.has_help():
        print(
            "build-watchlist sync: pull the local buildcost.db mirror from "
            "the remote (libsql sync).\n"
            "Use this when another process or machine has written to the "
            "buildcost remote since your last local read."
        )
        return True

    buildcost_db = DatabaseConfig("buildcost")
    print("Syncing buildcost local mirror from remote…")
    try:
        buildcost_db.sync()
    except Exception as exc:
        logger.error(f"buildcost sync failed: {exc}")
        print(f"Error: buildcost sync failed: {exc}")
        return False
    print(f"Synced buildcost local mirror ({buildcost_db.path})")
    return True


def _resolve_type_ids(p: ParsedArgs) -> list[int] | None:
    """Resolve the input type_ids from --type_id / --file / --paste.

    Returns None if the inputs are invalid or empty (already printed an error).
    """
    type_ids_str = p.get_string("type_id", "type-id")
    file_path = p.get_string("file")
    paste = p.has_flag("paste")

    sources = sum(1 for x in (type_ids_str, file_path, paste) if x)
    if sources > 1:
        print("Error: --type_id, --file, and --paste are mutually exclusive")
        return None
    if sources == 0:
        print("Error: provide --type_id=…, --file=…, or --paste")
        return None

    if type_ids_str:
        return _parse_csv_ids(type_ids_str)

    if file_path:
        return _read_type_ids_from_csv(file_path)

    pasted = get_multiline_input()
    if not pasted:
        print("Error: no paste input provided")
        return None
    return _parse_pasted_lines(pasted)


def _parse_csv_ids(value: str) -> list[int] | None:
    try:
        return [int(s.strip()) for s in value.split(",") if s.strip()]
    except ValueError as exc:
        print(f"Error: invalid type_id value: {exc}")
        return None


def _read_type_ids_from_csv(path: str) -> list[int] | None:
    try:
        type_ids: list[int] = []
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                val = (row.get("type_ids") or row.get("type_id") or "").strip()
                if val:
                    type_ids.append(int(val))
        if not type_ids:
            print(f"Error: no type_ids found in {path}")
            return None
        return type_ids
    except (OSError, ValueError) as exc:
        print(f"Error reading {path}: {exc}")
        return None


def _parse_pasted_lines(lines) -> list[int] | None:
    cleaned = [line.strip() for line in lines if line.strip()]
    if not cleaned:
        print("Error: paste input was empty")
        return None
    type_info_list = get_type_from_list(cleaned)
    type_ids = [info.type_id for info in type_info_list if info.type_id]
    if not type_ids:
        print("Error: no valid type names found in paste input")
        return None
    return type_ids


def _suggest(name: str, choices) -> str:
    matches = [c for c in choices if c.startswith(name[:1])]
    if not matches:
        return ""
    return f" Did you mean: {', '.join(sorted(matches))}?"


def _print_add_summary(result: AddResult, *, force: bool) -> None:
    print(f"build-watchlist add: {result.added} added")
    if result.skipped:
        verb = "added with --force" if force else "skipped (no blueprint)"
        if not force:
            print(f"  {len(result.skipped)} {verb}: {result.skipped[:10]}")
    if result.invalid:
        print(f"  {len(result.invalid)} invalid (not in SDE): {result.invalid[:10]}")


def _print_remove_summary(result: RemoveResult) -> None:
    print(f"build-watchlist remove: {result.removed} removed")
    if result.not_present:
        print(
            f"  {len(result.not_present)} not in build_watchlist: "
            f"{result.not_present[:10]}"
        )


def _print_mirror_summary(result: SyncResult) -> None:
    print(
        f"build-watchlist mirror: {result.market_size} in market, "
        f"{result.already_present} already present, {result.added} added"
    )
    if result.skipped:
        print(
            f"  {len(result.skipped)} skipped (no blueprint): "
            f"{result.skipped[:10]}"
        )
