"""
Module Equivalents CLI Manager

CLI commands for managing the module_equivalents table:
- list: Display all equivalence groups
- add: Create a new group from type IDs
- remove: Delete a group by ID
"""

from rich.console import Console
from rich.table import Table
from rich import box

from mkts_backend.config.logging_config import configure_logging
from mkts_backend.config.market_context import MarketContext
from mkts_backend.db.equiv_handlers import (
    list_equiv_groups,
    add_equiv_group,
    remove_equiv_group,
    resolve_type_name,
    resolve_type_id,
    find_equiv_by_attributes,
    ensure_equiv_table,
    sync_equiv_to_remote,
    seed_fit_equivs_from_csv,
    list_fit_equiv_groups,
)

logger = configure_logging(__name__)
console = Console()


def _get_target_markets(args: list[str], market_alias: str) -> list[str]:
    """
    Determine which markets to operate on.

    Defaults to ALL markets since equivalents are universal game data.
    Use --market=<alias> to target a single market.
    """
    for arg in args:
        if arg.startswith("--market="):
            return [arg.split("=", 1)[1]]
    # Default: all available markets
    return MarketContext.list_available()


def equiv_command(args: list[str], market_alias: str = "primary") -> bool:
    """
    Route equiv subcommands.

    Operates on ALL markets by default since module equivalents are
    universal EVE game data. Use --market=<alias> to target one market.

    Args:
        args: Command arguments (after 'equiv')
        market_alias: Market alias (overridden to all markets by default)

    Returns:
        True if command succeeded
    """
    target_aliases = _get_target_markets(args, market_alias)

    # Determine subcommand (first positional arg)
    subcommand = None
    for arg in args:
        if not arg.startswith("--"):
            subcommand = arg
            break

    if subcommand == "list":
        # List only needs one market (they should be identical)
        market_ctx = MarketContext.from_settings(target_aliases[0])
        return _equiv_list(market_ctx)
    elif subcommand == "add":
        return _equiv_add_all(args, target_aliases)
    elif subcommand == "remove":
        return _equiv_remove_all(args, target_aliases)
    elif subcommand == "find":
        return _equiv_find(args, target_aliases)
    elif subcommand == "seed-fit":
        return _equiv_seed_fit(args, target_aliases)
    elif subcommand == "list-fit":
        return _equiv_list_fit(args, target_aliases)
    else:
        _display_equiv_help()
        return True


def _equiv_list(market_ctx) -> bool:
    """List all equivalence groups."""
    groups = list_equiv_groups(market_ctx)

    if not groups:
        console.print("[yellow]No equivalence groups found.[/yellow]")
        return True

    table = Table(
        title="Module Equivalence Groups",
        box=box.ROUNDED,
        show_lines=True,
    )
    table.add_column("Group ID", style="cyan", justify="center")
    table.add_column("Type ID", style="dim")
    table.add_column("Module Name", style="green")

    for group in groups:
        gid = group["equiv_group_id"]
        for i, member in enumerate(group["members"]):
            table.add_row(
                str(gid),
                str(member["type_id"]),
                member["type_name"],
            )

    console.print(table)
    console.print(f"\n[dim]{len(groups)} group(s) found[/dim]")
    return True


def _equiv_add_all(args: list[str], target_aliases: list[str]) -> bool:
    """Add a new equivalence group to all target markets."""
    type_ids_str = None
    for arg in args:
        if arg.startswith("--type-ids="):
            type_ids_str = arg.split("=", 1)[1]

    if not type_ids_str:
        console.print("[red]Error: --type-ids is required[/red]")
        console.print("Usage: mkts-backend equiv add --type-ids=13984,17838,15705")
        return False

    try:
        type_ids = [int(tid.strip()) for tid in type_ids_str.split(",") if tid.strip()]
    except ValueError:
        console.print("[red]Error: --type-ids must be comma-separated integers[/red]")
        return False

    if len(type_ids) < 2:
        console.print("[red]Error: Need at least 2 type IDs for an equivalence group[/red]")
        return False

    # Preview what will be added
    console.print("\n[bold]Adding equivalence group:[/bold]")
    for tid in type_ids:
        name = resolve_type_name(tid)
        if name:
            console.print(f"  {tid}: {name}")
        else:
            console.print(f"  {tid}: [red]NOT FOUND in SDE[/red]")

    console.print(f"\n[bold]Target markets:[/bold] {', '.join(target_aliases)}")

    success = True
    for alias in target_aliases:
        market_ctx = MarketContext.from_settings(alias)
        ensure_equiv_table(market_ctx)
        new_group_id = add_equiv_group(type_ids, market_ctx)
        if new_group_id is None:
            console.print(f"  [yellow]{alias}[/yellow]: skipped - type IDs already in an existing group")
            success = False
        else:
            console.print(f"  [green]{alias}[/green]: created group {new_group_id}")

    return success


def _equiv_remove_all(args: list[str], target_aliases: list[str]) -> bool:
    """Remove an equivalence group from all target markets."""
    group_id = None
    for arg in args:
        if arg.startswith("--id="):
            try:
                group_id = int(arg.split("=", 1)[1])
            except ValueError:
                console.print("[red]Error: --id must be an integer[/red]")
                return False

    if group_id is None:
        console.print("[red]Error: --id is required[/red]")
        console.print("Usage: mkts-backend equiv remove --id=1")
        return False

    console.print(f"[bold]Target markets:[/bold] {', '.join(target_aliases)}")

    for alias in target_aliases:
        market_ctx = MarketContext.from_settings(alias)
        count = remove_equiv_group(group_id, market_ctx)
        if count > 0:
            console.print(f"  [green]{alias}[/green]: removed group {group_id} ({count} entries)")
        else:
            console.print(f"  [yellow]{alias}[/yellow]: no entries for group {group_id}")

    return True


def _equiv_find(args: list[str], target_aliases: list[str]) -> bool:
    """Find equivalent modules by attribute fingerprinting."""
    type_id = None
    name_query = None
    do_add = "--add" in args

    # Parse --type-id= flag
    for arg in args:
        if arg.startswith("--type-id="):
            try:
                type_id = int(arg.split("=", 1)[1])
            except ValueError:
                console.print("[red]Error: --type-id must be an integer[/red]")
                return False
        elif arg.startswith("--name="):
            name_query = arg.split("=", 1)[1].strip('"').strip("'")

    # Positional arg: first non-flag arg after "find"
    if type_id is None and name_query is None:
        found_find = False
        for arg in args:
            if arg.startswith("--"):
                continue
            if not found_find:
                if arg == "find":
                    found_find = True
                continue
            # First positional after "find"
            try:
                type_id = int(arg)
            except ValueError:
                name_query = arg
            break

    if type_id is None and name_query is None:
        console.print("[red]Error: Provide a type ID or module name[/red]")
        console.print("Usage: mkts-backend equiv find <type_id|name> [--add]")
        return False

    # Resolve name to type ID if needed
    if type_id is None:
        matches = resolve_type_id(name_query)
        if not matches:
            console.print(f"[red]No types found matching '{name_query}'[/red]")
            return False
        if len(matches) == 1:
            type_id = matches[0][0]
            console.print(f"Matched: [bold]{matches[0][1]}[/bold] ({type_id})")
        else:
            # Multiple matches — show selection table
            table = Table(title=f"Multiple matches for '{name_query}'", box=box.SIMPLE)
            table.add_column("Type ID", style="cyan")
            table.add_column("Name", style="green")
            for tid, tname in matches:
                table.add_row(str(tid), tname)
            console.print(table)
            console.print("\n[yellow]Multiple matches. Use --type-id=<id> to select one.[/yellow]")
            return True

    # Find equivalents by attribute fingerprint
    ref_name = resolve_type_name(type_id)
    if ref_name is None:
        console.print(f"[red]Type ID {type_id} not found in SDE[/red]")
        return False

    console.print(f"\nFinding equivalents for [bold]{ref_name}[/bold] ({type_id})...\n")
    results = find_equiv_by_attributes(type_id)

    if not results:
        console.print("[yellow]No equivalent modules found.[/yellow]")
        return True

    if len(results) == 1:
        console.print("[yellow]No other equivalent modules found (only the reference type matched).[/yellow]")
        return True

    # Display results
    table = Table(
        title=f"Equivalent Modules ({len(results)} found)",
        box=box.ROUNDED,
    )
    table.add_column("Type ID", style="cyan", justify="right")
    table.add_column("Module Name", style="green")
    table.add_column("Group", style="dim")
    table.add_column("Meta", style="dim")

    for r in results:
        name_style = "bold green" if r["typeID"] == type_id else "green"
        table.add_row(
            str(r["typeID"]),
            f"[{name_style}]{r['typeName']}[/{name_style}]",
            r["groupName"] or "",
            r["metaGroupName"] or "",
        )

    console.print(table)

    # Auto-add if --add flag
    if do_add:
        equiv_type_ids = [r["typeID"] for r in results]
        console.print(f"\n[bold]Adding equivalence group to: {', '.join(target_aliases)}[/bold]")
        for alias in target_aliases:
            market_ctx = MarketContext.from_settings(alias)
            ensure_equiv_table(market_ctx)
            new_group_id = add_equiv_group(equiv_type_ids, market_ctx)
            if new_group_id is None:
                console.print(f"  [yellow]{alias}[/yellow]: skipped - type IDs already in an existing group")
            else:
                console.print(f"  [green]{alias}[/green]: created group {new_group_id}")

    return True


def _equiv_seed_fit(args: list[str], target_aliases: list[str]) -> bool:
    """Seed fit-scoped equivalents from a CSV file."""
    csv_path = None
    fit_id = None

    for arg in args:
        if arg.startswith("--csv="):
            csv_path = arg.split("=", 1)[1]
        elif arg.startswith("--fit-id="):
            try:
                fit_id = int(arg.split("=", 1)[1])
            except ValueError:
                console.print("[red]Error: --fit-id must be an integer[/red]")
                return False

    if not csv_path or fit_id is None:
        console.print("[red]Error: --csv and --fit-id are required[/red]")
        console.print("Usage: mkts-backend equiv seed-fit --csv=equiv.csv --fit-id=993")
        return False

    console.print(f"[bold]Seeding fit-scoped equivalents for fit {fit_id} from {csv_path}[/bold]")
    console.print(f"[bold]Target markets:[/bold] {', '.join(target_aliases)}")

    for alias in target_aliases:
        market_ctx = MarketContext.from_settings(alias)
        count = seed_fit_equivs_from_csv(csv_path, fit_id, market_ctx)
        console.print(f"  [green]{alias}[/green]: seeded {count} group(s)")

    return True


def _equiv_list_fit(args: list[str], target_aliases: list[str]) -> bool:
    """List fit-scoped equivalence groups."""
    fit_id = None
    for arg in args:
        if arg.startswith("--fit-id="):
            try:
                fit_id = int(arg.split("=", 1)[1])
            except ValueError:
                console.print("[red]Error: --fit-id must be an integer[/red]")
                return False

    if fit_id is None:
        console.print("[red]Error: --fit-id is required[/red]")
        console.print("Usage: mkts-backend equiv list-fit --fit-id=993")
        return False

    market_ctx = MarketContext.from_settings(target_aliases[0])
    groups = list_fit_equiv_groups(fit_id, market_ctx)

    if not groups:
        console.print(f"[yellow]No fit-scoped equivalence groups for fit {fit_id}.[/yellow]")
        return True

    table = Table(
        title=f"Fit-Scoped Equivalence Groups (fit {fit_id})",
        box=box.ROUNDED,
        show_lines=True,
    )
    table.add_column("Group ID", style="cyan", justify="center")
    table.add_column("Type ID", style="dim")
    table.add_column("Module Name", style="green")

    for group in groups:
        gid = group["equiv_group_id"]
        for member in group["members"]:
            table.add_row(str(gid), str(member["type_id"]), member["type_name"])

    console.print(table)
    console.print(f"\n[dim]{len(groups)} group(s) found[/dim]")
    return True


def _display_equiv_help():
    """Display help for the equiv subcommand."""
    console.print("""
[bold]equiv[/bold] - Manage module equivalence groups

[bold]USAGE:[/bold]
    mkts-backend equiv <subcommand> [options]

[bold]SUBCOMMANDS:[/bold]
    list                           List all global equivalence groups
    find <type_id|name> [--add]    Auto-discover equivalent modules by attributes
    add --type-ids=<id1,id2,...>   Create a new global group (resolves names from SDE)
    remove --id=<group_id>         Remove a global group
    seed-fit --csv=<path> --fit-id=<id>   Seed fit-scoped equivalents from CSV
    list-fit --fit-id=<id>                List fit-scoped equivalence groups

[bold]OPTIONS:[/bold]
    --market=<alias>   Target a single market (default: all markets)
    --help             Show this help

[bold]NOTE:[/bold]
    Module equivalents are universal EVE game data, so add/remove
    operates on ALL markets by default. Use --market to target one.

[bold]EXAMPLES:[/bold]
    mkts-backend equiv list
    mkts-backend equiv find 13984
    mkts-backend equiv find "Thermal Armor Hardener"
    mkts-backend equiv find 13984 --add
    mkts-backend equiv add --type-ids=13984,17838,15705,28528,14065,13982
    mkts-backend equiv remove --id=1
""")
