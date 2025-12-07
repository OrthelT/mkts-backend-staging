import re
import json
from dataclasses import dataclass, field
from typing import Generator, Optional, Tuple, List, Dict
from collections import defaultdict
from datetime import datetime, timezone

from sqlalchemy import create_engine, text
import pandas as pd

from mkts_backend.config.logging_config import configure_logging
from mkts_backend.config import DatabaseConfig
from mkts_backend.utils.doctrine_update import (
    DoctrineFit,
    upsert_doctrine_fits,
    upsert_doctrine_map,
    upsert_ship_target,
    refresh_doctrines_for_fit,
)
from mkts_backend.utils.db_utils import add_missing_items_to_watchlist

logger = configure_logging(__name__)

# Database configs (keep objects; avoid overwriting with URLs)
_wcmkt_db = DatabaseConfig("wcmkt")
_sde_db = DatabaseConfig("sde")
_fittings_db = DatabaseConfig("fittings")

def _get_engine(db_alias: str, remote: bool = False):
    cfg = DatabaseConfig(db_alias)
    return cfg.remote_engine if remote else cfg.engine


@dataclass
class FittingItem:
    flag: str
    quantity: int
    fit_id: int
    type_name: str
    ship_type_name: str
    fit_name: Optional[str] = None

    type_id: int = field(init=False)
    type_fk_id: int = field(init=False)

    def __post_init__(self) -> None:
        self.type_id = self.get_type_id()
        self.type_fk_id = self.type_id
        self.details = self.get_fitting_details()
        if "description" in self.details:
            self.description = self.details['description']
        else:
            self.description = "No description"

        if self.fit_name is None:
            if "name" in self.details:
                self.fit_name = self.details["name"]
                if "name" in self.details and self.fit_name != self.details["name"]:
                    logger.warning(
                        f"Fit name mismatch: parsed='{self.fit_name}' vs DB='{self.details['name']}'"
                    )
            else:
                self.fit_name = f"Default {self.ship_type_name} fit"

    def get_type_id(self) -> int:
        engine = _sde_db.engine
        query = text("SELECT typeID FROM inv_info WHERE typeName = :type_name")
        with engine.connect() as conn:
            result = conn.execute(query, {"type_name": self.type_name}).fetchone()
            return result[0] if result else -1

    def get_fitting_details(self) -> dict:
        engine = _fittings_db.engine
        query = text("SELECT * FROM fittings_fitting WHERE id = :fit_id")
        with engine.connect() as conn:
            row = conn.execute(query, {"fit_id": self.fit_id}).fetchone()
            return dict(row._mapping) if row else {}

@dataclass
class FitMetadata:
    description: str
    name: str
    fit_id: int
    doctrine_id: int
    target: int
    ship_type_id: Optional[int] = None
    ship_name: Optional[str] = None
    last_updated: datetime = field(init=False)
    def __post_init__(self):
        self.last_updated = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class FitParseResult:
    items: List[Dict]
    ship_name: str
    fit_name: str
    missing_types: List[str]


def convert_fit_date(date: str) -> datetime:
    dt = datetime.strptime("15 Jan 2025 19:12:04", "%d %b %Y %H:%M:%S")
    return dt


def slot_yielder() -> Generator[str, None, None]:
    corrected_order = ['LoSlot', 'MedSlot', 'HiSlot', 'RigSlot', 'DroneBay']
    for slot in corrected_order:
        yield slot
    while True:
        yield 'Cargo'


def _lookup_type_id(type_name: str, conn) -> Optional[int]:
    result = conn.execute(
        text("SELECT typeID FROM inv_info WHERE typeName = :type_name"),
        {"type_name": type_name},
    ).fetchone()
    return result[0] if result else None


def _resolve_ship_type_id(ship_name: str, conn) -> Optional[int]:
    result = conn.execute(
        text("SELECT typeID FROM inv_info WHERE typeName = :type_name"),
        {"type_name": ship_name},
    ).fetchone()
    return result[0] if result else None


def parse_eft_fit_file(fit_file: str, fit_id: int, sde_engine) -> FitParseResult:
    """
    Parse an EFT-formatted fit file into structured items.

    Returns:
        FitParseResult with:
        - items: list of dicts matching fittings_fittingitem columns
        - ship_name, fit_name
        - missing_types: items we could not resolve to a type_id
    """
    items: List[Dict] = []
    missing: List[str] = []
    slot_gen = slot_yielder()
    current_slot = None
    ship_name = ""
    fit_name = ""
    slot_counters = defaultdict(int)

    with sde_engine.connect() as sde_conn:
        with open(fit_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()

                if line.startswith("[") and line.endswith("]"):
                    clean_name = line.strip("[]")
                    parts = clean_name.split(",")
                    ship_name = parts[0].strip()
                    fit_name = parts[1].strip() if len(parts) > 1 else "Unnamed Fit"
                    continue

                if line == "":
                    current_slot = next(slot_gen)
                    continue

                if current_slot is None:
                    current_slot = next(slot_gen)

                qty_match = re.search(r"\\s+x(\\d+)$", line)
                if qty_match:
                    qty = int(qty_match.group(1))
                    item_name = line[: qty_match.start()].strip()
                else:
                    qty = 1
                    item_name = line.strip()

                if current_slot in {"LoSlot", "MedSlot", "HiSlot", "RigSlot"}:
                    suffix = slot_counters[current_slot]
                    slot_counters[current_slot] += 1
                    slot_name = f\"{current_slot}{suffix}\"
                else:
                    slot_name = current_slot

                type_id = _lookup_type_id(item_name, sde_conn)
                if type_id is None:
                    missing.append(item_name)
                    logger.warning(f\"Unable to resolve type_id for '{item_name}' (fit {fit_id})\")
                    continue

                items.append(
                    {
                        \"flag\": slot_name,
                        \"quantity\": qty,
                        \"type_id\": type_id,
                        \"fit_id\": fit_id,
                        \"type_fk_id\": type_id,
                    }
                )

    return FitParseResult(items=items, ship_name=ship_name, fit_name=fit_name, missing_types=missing)


def process_fit(fit_file: str, fit_id: int):
    fit = []
    qty = 1
    slot_gen = slot_yielder()
    current_slot = None
    ship_name = ""
    fit_name = ""
    slot_counters = defaultdict(int)

    with open(fit_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()

            if line.startswith("[") and line.endswith("]"):
                clean_name = line.strip('[]')
                parts = clean_name.split(',')
                ship_name = parts[0].strip()
                fit_name = parts[1].strip() if len(parts) > 1 else "Unnamed Fit"
                continue

            if line == "":
                current_slot = next(slot_gen)
                continue

            if current_slot is None:
                current_slot = next(slot_gen)

            qty_match = re.search(r'\s+x(\d+)$', line)
            if qty_match:
                qty = int(qty_match.group(1))
                item = line[:qty_match.start()].strip()
            else:
                qty = 1
                item = line.strip()

            if current_slot in {'LoSlot', 'MedSlot', 'HiSlot', 'RigSlot'}:
                suffix = slot_counters[current_slot]
                slot_counters[current_slot] += 1
                slot_name = f"{current_slot}{suffix}"
            else:
                slot_name = current_slot

            fitting_item = FittingItem(
                flag=slot_name,
                fit_id=fit_id,
                type_name=item,
                ship_type_name=ship_name,
                fit_name=fit_name,
                quantity=qty,
            )

            fit.append([fitting_item.flag, fitting_item.quantity, fitting_item.type_id, fit_id, fitting_item.type_id])

    return fit, ship_name, fit_name


def add_doctrine_to_watch(doctrine_id: int, remote: bool = False) -> None:
    """
    Add a doctrine from fittings_doctrine to watch_doctrines table.

    Args:
        doctrine_id: The doctrine ID to copy from fittings_doctrine to watch_doctrines
    """
    db = DatabaseConfig("fittings")
    engine = db.remote_engine if remote else db.engine

    with engine.connect() as conn:
        # Check if doctrine exists in fittings_doctrine
        select_stmt = text("SELECT * FROM fittings_doctrine WHERE id = :doctrine_id")
        result = conn.execute(select_stmt, {"doctrine_id": doctrine_id})
        doctrine_row = result.fetchone()

        if not doctrine_row:
            logger.error(f"Doctrine {doctrine_id} not found in fittings_doctrine")
            return

        # Check if already exists in watch_doctrines
        check_stmt = text("SELECT COUNT(*) FROM watch_doctrines WHERE id = :doctrine_id")
        result = conn.execute(check_stmt, {"doctrine_id": doctrine_id})
        count = result.fetchone()[0]

        if count > 0:
            logger.info(f"Doctrine {doctrine_id} already exists in watch_doctrines")
            return

        # Insert into watch_doctrines
        insert_stmt = text("""
            INSERT INTO watch_doctrines (id, name, icon_url, description, created, last_updated)
            VALUES (:id, :name, :icon_url, :description, :created, :last_updated)
        """)

        conn.execute(insert_stmt, {
            "id": doctrine_row[0],
            "name": doctrine_row[1],
            "icon_url": doctrine_row[2],
            "description": doctrine_row[3],
            "created": doctrine_row[4],
            "last_updated": doctrine_row[5]
        })
        conn.commit()

        logger.info(f"Added doctrine {doctrine_id} ('{doctrine_row[1]}') to watch_doctrines")

    engine.dispose()


def insert_fit_items_to_db(fit_items: list, fit_id: int, clear_existing: bool = True, remote: bool = False) -> None:
    """
    Insert parsed fit items into the fittings_fittingitem table.

    Args:
        fit_items: List of fit items where each item is [flag, quantity, type_id, fit_id, type_fk_id]
        fit_id: The fit ID these items belong to
        clear_existing: If True, delete existing items for this fit_id before inserting
    """
    engine = _get_engine("fittings", remote)

    with engine.connect() as conn:
        # Disable foreign key constraints for this transaction
        conn.execute(text("PRAGMA foreign_keys = OFF"))

        # Optionally clear existing items for this fit
        if clear_existing:
            delete_stmt = text("DELETE FROM fittings_fittingitem WHERE fit_id = :fit_id")
            conn.execute(delete_stmt, {"fit_id": fit_id})
            logger.info(f"Cleared existing items for fit_id {fit_id}")

        # Insert new items
        insert_stmt = text("""
            INSERT INTO fittings_fittingitem (flag, quantity, type_id, fit_id, type_fk_id)
            VALUES (:flag, :quantity, :type_id, :fit_id, :type_fk_id)
        """)

        for item in fit_items:
            if isinstance(item, dict):
                flag = item.get("flag")
                quantity = item.get("quantity")
                type_id = item.get("type_id")
                type_fk_id = item.get("type_fk_id")
            else:
                flag, quantity, type_id, fit_id, type_fk_id = item

            if type_id is None:
                logger.warning(f"Skipping item with missing type_id: {item}")
                continue

            conn.execute(
                insert_stmt,
                {
                    "flag": flag,
                    "quantity": quantity,
                    "type_id": type_id,
                    "fit_id": fit_id,
                    "type_fk_id": type_fk_id,
                },
            )

        conn.commit()

        # Re-enable foreign key constraints
        conn.execute(text("PRAGMA foreign_keys = ON"))

        logger.info(f"Inserted {len(fit_items)} items for fit_id {fit_id}")

    engine.dispose()

def parse_fit_metadata(fit_metadata_file: str) -> FitMetadata:
    with open(fit_metadata_file, 'r', encoding='utf-8') as f:
        metadata = json.load(f)
    return FitMetadata(**metadata)

def upsert_fittings_fitting(metadata: FitMetadata, ship_type_id: int, remote: bool = False) -> None:
    """
    Upsert the shell record in fittings_fitting.
    """
    engine = _get_engine("fittings", remote)
    now = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    created = now
    last_updated = metadata.last_updated or now
    with engine.connect() as conn:
        stmt = text(
            """
            INSERT INTO fittings_fitting (id, description, name, ship_type_type_id, ship_type_id, created, last_updated)
            VALUES (:id, :description, :name, :ship_type_type_id, :ship_type_id, :created, :last_updated)
            ON CONFLICT(id) DO UPDATE SET
                description = excluded.description,
                name = excluded.name,
                ship_type_type_id = excluded.ship_type_type_id,
                ship_type_id = excluded.ship_type_id,
                last_updated = excluded.last_updated
            """
        )
        conn.execute(
            stmt,
            {
                "id": metadata.fit_id,
                "description": metadata.description,
                "name": metadata.name,
                "ship_type_type_id": ship_type_id,
                "ship_type_id": ship_type_id,
                "created": created,
                "last_updated": last_updated,
            },
        )
        conn.commit()
    logger.info(
        f"Upserted fittings_fitting for fit_id {metadata.fit_id}: {metadata.name} (ship_type_id={ship_type_id})"
    )


def ensure_doctrine_link(doctrine_id: int, fit_id: int, remote: bool = False) -> None:
    engine = _get_engine("fittings", remote)
    with engine.connect() as conn:
        exists = conn.execute(
            text(
                "SELECT 1 FROM fittings_doctrine_fittings WHERE doctrine_id = :doctrine_id AND fitting_id = :fit_id"
            ),
            {"doctrine_id": doctrine_id, "fit_id": fit_id},
        ).fetchone()
        if exists:
            return
        conn.execute(
            text(
                "INSERT INTO fittings_doctrine_fittings (doctrine_id, fitting_id) VALUES (:doctrine_id, :fit_id)"
            ),
            {"doctrine_id": doctrine_id, "fit_id": fit_id},
        )
        conn.commit()
    logger.info(f"Linked doctrine_id {doctrine_id} to fit_id {fit_id} in fittings_doctrine_fittings")


def update_fit_workflow(
    fit_id: int,
    fit_file: str,
    fit_metadata_file: str,
    remote: bool = False,
    clear_existing: bool = True,
    dry_run: bool = False,
):
    """
    End-to-end update for a fit:
    - Parse EFT file
    - Upsert fittings_fitting and fittings_fittingitem
    - Ensure doctrine link in fittings_doctrine_fittings
    - Propagate to wcmktprod doctrine tables and watchlist
    """
    metadata = parse_fit_metadata(fit_metadata_file)
    sde_engine = _get_engine("sde", False)

    parse_result = parse_eft_fit_file(fit_file, fit_id, sde_engine)
    metadata.ship_name = parse_result.ship_name

    with sde_engine.connect() as conn:
        ship_type_id = metadata.ship_type_id or _resolve_ship_type_id(parse_result.ship_name, conn)

    if ship_type_id is None:
        raise ValueError(f"Could not resolve ship type id for ship '{parse_result.ship_name}'")

    if dry_run:
        return {
            "fit_id": fit_id,
            "ship_name": parse_result.ship_name,
            "ship_type_id": ship_type_id,
            "items": parse_result.items,
            "missing_items": parse_result.missing_types,
        }

    # Upsert core fitting data
    upsert_fittings_fitting(metadata, ship_type_id, remote=remote)
    insert_fit_items_to_db(parse_result.items, fit_id=fit_id, clear_existing=clear_existing, remote=remote)
    ensure_doctrine_link(metadata.doctrine_id, fit_id, remote=remote)

    doctrine_fit = DoctrineFit(doctrine_id=metadata.doctrine_id, fit_id=fit_id, target=metadata.target)

    # Propagate to market/production dbs
    upsert_doctrine_fits(doctrine_fit, remote=remote)
    upsert_doctrine_map(doctrine_fit.doctrine_id, doctrine_fit.fit_id, remote=remote)
    upsert_ship_target(
        fit_id=doctrine_fit.fit_id,
        fit_name=doctrine_fit.fit_name,
        ship_id=doctrine_fit.ship_type_id,
        ship_name=doctrine_fit.ship_name,
        ship_target=metadata.target,
        remote=remote,
    )
    refresh_doctrines_for_fit(
        fit_id=doctrine_fit.fit_id,
        ship_id=doctrine_fit.ship_type_id,
        ship_name=doctrine_fit.ship_name,
        remote=remote,
    )

    # Add missing items to watchlist in wcmkt
    type_ids = {item["type_id"] for item in parse_result.items}
    type_ids.add(ship_type_id)
    add_missing_items_to_watchlist(list(type_ids), remote=remote)
    logger.info(
        f"Completed fit update for fit_id={fit_id}, doctrine_id={metadata.doctrine_id} (remote={remote})"
    )


def update_existing_fit(fit_id: int, fit_file: str, fit_metadata_file: str, remote: bool = False, clear_existing: bool = True):
    update_fit_workflow(fit_id, fit_file, fit_metadata_file, remote=remote, clear_existing=clear_existing)


def update_fit(fit_id: int, fit_file: str, fit_metadata_file: str, remote: bool = False, clear_existing: bool = True):
    update_fit_workflow(fit_id, fit_file, fit_metadata_file, remote=remote, clear_existing=clear_existing)

if __name__ == "__main__":
    pass