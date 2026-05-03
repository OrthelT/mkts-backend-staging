# Independent build_watchlist with manual management CLI

**Status:** approved
**Date:** 2026-05-03
**Author:** Daisy + Claude (brainstorming session)

## Problem

`build_watchlist` is currently a derived view of `wcmktprod.watchlist`, rebuilt
on every `update-builder-costs` run. This couples buildcost cost data to the
market watchlist's lifecycle: items removed from wcmktprod disappear from
buildcost, and there's no way to track items in buildcost that aren't actively
on a market (e.g., items the alliance plans to manufacture but doesn't yet
trade).

## Goal

Make `build_watchlist` an independent table with its own lifecycle:

- `wcmktprod.watchlist` ⊆ `build_watchlist` (the market watchlist is a subset)
- Items added via the existing `add_watchlist` are auto-mirrored into
  `build_watchlist` (buildable filter applied)
- Manual add/remove/sync operations live behind a new
  `build-watchlist [add|remove|sync]` CLI command following the existing
  `[command] [subcommand]` convention used by `equiv` and `fit-update`

## Out of scope

- Pruning items from `build_watchlist` when they leave wcmktprod — by design,
  buildcost rows persist until explicitly removed.
- `build-watchlist list` or other read-only views — YAGNI.
- Cleaning up the existing `add_watchlist`'s `inv_info` vs `sdetypes` SDE
  drift (flagged in project memory). New code in this spec uses `sdetypes`;
  existing code path is untouched.
- Source tracking (manual vs mirrored). No `source` column.

## Architecture

### Data flow changes

**Before:**
```
update-builder-costs → rebuild build_watchlist from wcmktprod.watchlist
                     → fetch costs from EverRef
                     → upsert builder_costs
```

**After:**
```
add_watchlist (existing)            → write to wcmktprod.watchlist
                                    → mirror buildable items to build_watchlist

build-watchlist add                 → write to build_watchlist (force-able)
build-watchlist remove              → delete from build_watchlist
build-watchlist mirror                → diff wcmktprod into build_watchlist

update-builder-costs (modified)     → read build_watchlist (no rebuild)
                                    → fetch costs from EverRef
                                    → upsert builder_costs
```

### Module layout (`src/mkts_backend/builder_costs/`)

| File | Responsibility | Changes |
|------|----------------|---------|
| `runner.py` | Orchestrate cost refresh | Drop `refresh_build_watchlist` step; read from `repository.read_build_watchlist` |
| `watchlist_sync.py` | Watchlist mutations | Replace `refresh_build_watchlist` with `add_to_build_watchlist`, `remove_from_build_watchlist`, `sync_from_market` |
| `repository.py` | buildcost.db I/O | Add `read_build_watchlist`, `read_build_watchlist_type_ids`, `delete_build_watchlist_rows` |
| `sde_lookup.py` *(new)* | SDE metadata for buildcost flow | `lookup_type_metadata(type_ids, sde_db) -> dict[int, dict]` using `sdetypes` |
| `__init__.py` | Package init | Unchanged |

### Buildable-filter sharing

The existing `_get_meta_groups` in `esi/async_everref.py` joins
`industryActivityProducts` and returns `dict[type_id, meta_group_id]` — used by
the cost fetch to pick ME/runs values. Add a sibling helper that uses the same
SQL but returns set membership:

```python
def filter_buildable(type_ids: list[int], sde_engine: Engine) -> set[int]:
    """Return type_ids that have a manufacturing blueprint in SDE."""
```

Both helpers share the chunked-IN query (extract a `_buildable_query` private
helper to avoid duplication). `add_to_build_watchlist` and `sync_from_market`
call `filter_buildable`. The cost fetch keeps using `_get_meta_groups`.

### CLI

Single registered command `build-watchlist`, dispatched on first positional:

```
mkts-backend build-watchlist add    --type_id=12345,67890 [--file=…] [--paste] [--force]
mkts-backend build-watchlist remove --type_id=12345,67890 [--file=…] [--paste]
mkts-backend build-watchlist mirror   (reconcile from wcmktprod.watchlist)
mkts-backend build-watchlist sync     (pull buildcost local mirror; thin db.sync())
mkts-backend build-watchlist --help
mkts-backend build-watchlist <subcommand> --help
```

`mirror` and `sync` are deliberately separate verbs to avoid the semantic
collision where elsewhere in the CLI ``sync`` always means
``DatabaseConfig.sync()``. ``mirror`` is the wcmktprod→buildcost
reconciliation; ``sync`` is the libsql remote→local pull.

Dispatch shape mirrors `_handle_fit_update` in `command_registry.py`:

```python
def _handle_build_watchlist(args: list[str], market_alias: str) -> bool:
    p = ParsedArgs(args)
    if p.has_help() and not p.positionals():
        # top-level help
        ...
    subcommand = next((a for a in p.positionals() if a != "build-watchlist"), None)
    if not subcommand:
        # error: subcommand required
        ...
    handler = {"add": _add, "remove": _remove, "sync": _sync}.get(subcommand)
    if handler is None:
        # "Did you mean?" via existing Levenshtein logic
        ...
    return handler(p)
```

`add_watchlist` (existing) gets a small addition at the tail: after a
successful market write, call `add_to_build_watchlist(type_ids, force=False)`
once per command and print the mirror summary. No new flag.

## Components

### `add_to_build_watchlist`

```python
@dataclass
class AddResult:
    added: int          # rows upserted to build_watchlist
    skipped: list[int]  # type_ids dropped by the buildable filter
    invalid: list[int]  # type_ids not found in SDE at all

def add_to_build_watchlist(
    buildcost_db: DatabaseConfig,
    sde_db: DatabaseConfig,
    type_ids: list[int],
    *,
    force: bool = False,
) -> AddResult:
    """Add items to build_watchlist after looking up SDE metadata.

    With force=False, items not produced by a manufacturing blueprint are
    skipped (logged per-item) and reported in AddResult.skipped.
    With force=True, the buildable filter is bypassed; only SDE-missing
    items end up in AddResult.invalid.
    """
```

Shared by `build-watchlist add` and the auto-mirror in `add_watchlist`.

### `remove_from_build_watchlist`

```python
@dataclass
class RemoveResult:
    removed: int
    not_present: list[int]

def remove_from_build_watchlist(
    buildcost_db: DatabaseConfig,
    type_ids: list[int],
) -> RemoveResult:
    """Delete rows from build_watchlist. Idempotent for missing type_ids."""
```

### `sync_from_market`

```python
@dataclass
class SyncResult:
    market_size: int       # rows in market watchlist
    already_present: int   # in both market and build_watchlist
    added: int             # newly written to build_watchlist
    skipped: list[int]     # in market, missing from build_watchlist, not buildable

def sync_from_market(
    buildcost_db: DatabaseConfig,
    sde_db: DatabaseConfig,
    market_db: DatabaseConfig,
) -> SyncResult:
    """Add wcmktprod.watchlist items missing from build_watchlist (buildable only)."""
```

### `read_build_watchlist`

```python
def read_build_watchlist(buildcost_db: DatabaseConfig) -> list[dict]:
    """Return all rows from build_watchlist as dicts (type_id, type_name, group_name, category_id)."""

def read_build_watchlist_type_ids(buildcost_db: DatabaseConfig) -> set[int]:
    """Return just the type_ids — used by sync diff."""
```

`runner.run()` calls `read_build_watchlist` to build its `items` list and
metadata dict (replacing the data previously returned by
`refresh_build_watchlist`).

## Error handling

- Empty `build_watchlist` in `update-builder-costs` → log error, abort. Operator
  is told to run `build-watchlist mirror` or `build-watchlist add`.
- Empty input to `build-watchlist add/remove` → print usage, return False.
- SDE lookup failure for a type_id → counted as `invalid`, included in summary,
  doesn't block the rest of the batch.
- `--force` on `add` does not skip the SDE lookup itself; it only skips the
  buildable filter. Items missing from SDE entirely still land in `invalid`.

## Testing

New / modified test files under `tests/`:

- `test_build_watchlist_cli.py` *(new)* — routing for
  `build-watchlist add|remove|sync|--help`, unknown subcommand error path,
  mutually-exclusive `--type_id` vs `--file`, `--force` toggling the filter.
- `test_watchlist_sync.py` *(new)* — unit tests for
  `add_to_build_watchlist` (buildable split, force override, SDE-missing
  invalid path), `remove_from_build_watchlist` (idempotency), and
  `sync_from_market` (diff math against an existing `build_watchlist`).
- `test_async_everref.py` — add a test for the new `filter_buildable` helper;
  existing tests remain green.
- `test_cli_routing.py` — keep the `update-builder-costs` test as-is
  (`runner.run()` signature unchanged); add a routing test for
  `build-watchlist`.

## Migration

None. `build_watchlist` already exists in Turso and locally; no schema change.
First post-deploy run of `update-builder-costs` will skip the rebuild and
refresh costs for the rows currently in the table. If the table is in a stale
state, run `build-watchlist mirror` once to reconcile from wcmktprod.

## Risks

- **Drift accumulation:** because we never auto-prune, `build_watchlist` will
  grow as items churn through wcmktprod over time. Acceptable per design;
  `build-watchlist remove` is the escape valve.
- **EverRef rejection on `--force` adds:** items added with `--force` will
  produce per-item HTTP 400 warnings on each daily run. Acceptable — the
  operator opted in.
- **`add_watchlist` mirror failure:** if the buildcost mirror fails after a
  successful market write, the market-side write must NOT be rolled back.
  The mirror is best-effort; failure logs a warning and the command still
  reports success for the market write.
