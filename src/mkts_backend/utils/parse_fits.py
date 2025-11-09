import re
from dataclasses import dataclass, field
from typing import Optional, Generator
from collections import defaultdict
from datetime import datetime
import libsql
import pandas as pd
from sqlalchemy import create_engine, text
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.config import DatabaseConfig

db = DatabaseConfig("wcmkt")
sde_db = DatabaseConfig("sde")
fittings_db = DatabaseConfig("fittings")

mkt_db = db.url
sde_db = sde_db.url
fittings_db = fittings_db.url

logger = configure_logging(__name__)


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
        db = DatabaseConfig("sde")
        engine = db.engine
        query = text("SELECT typeID FROM inv_info WHERE typeName = :type_name")
        with engine.connect() as conn:
            result = conn.execute(query, {"type_name": self.type_name}).fetchone()
            return result[0] if result else -1

    def get_fitting_details(self) -> dict:
        engine = create_engine(fittings_db, echo=False)
        query = text("SELECT * FROM fittings_fitting WHERE id = :fit_id")
        with engine.connect() as conn:
            row = conn.execute(query, {"fit_id": self.fit_id}).fetchone()
            return dict(row._mapping) if row else {}


@dataclass
class DoctrineFit:
    doctrine_id: int
    fit_id: int
    target: int
    doctrine_name: str = field(init=False)
    fit_name: str = field(init=False)
    ship_type_id: int = field(init=False)
    ship_name: str = field(init=False)

    def __post_init__(self):
        self.doctrine_name = self.get_doctrine_name()
        self.fit_name = self.get_fit_name()
        self.ship_type_id = self.get_ship_type_id()
        self.ship_name = self.get_ship_name()

    def get_doctrine_name(self):
        db = DatabaseConfig("fittings")
        engine = db.engine
        with engine.connect() as conn:
            stmt = text("SELECT * FROM fittings_doctrine WHERE id = :doctrine_id")
            result = conn.execute(stmt, {"doctrine_id": self.doctrine_id})
            name = result.fetchone()[1]
            return name.strip()

    def get_ship_type_id(self):
        db = DatabaseConfig("fittings")
        engine = db.engine
        with engine.connect() as conn:
            stmt = text("SELECT * FROM fittings_fitting WHERE id = :fit_id")
            result = conn.execute(stmt, {"fit_id": self.fit_id})
            type_id = result.fetchone()[4]
            return type_id

    def get_fit_name(self):
        db = DatabaseConfig("fittings")
        engine = db.engine
        with engine.connect() as conn:
            stmt = text("SELECT * FROM fittings_fitting WHERE id = :fit_id")
            result = conn.execute(stmt, {"fit_id": self.fit_id})
            name = result.fetchone()[2]
            return name.strip()

    def get_ship_name(self, remote=False):
        db = DatabaseConfig("sde")
        engine = db.engine
        with engine.connect() as conn:
            stmt = text("SELECT * FROM inv_info WHERE typeID = :type_id")
            result = conn.execute(stmt, {"type_id": self.ship_type_id})
            name = result.fetchone()[1]
            return name.strip()

    def add_wcmkts2_doctrine_fits(self, remote=False):
        db = DatabaseConfig("wcmkt")
        engine = db.remote_engine if remote else db.engine
        with engine.connect() as conn:
            stmt = text("SELECT * FROM doctrine_fits")
            df = pd.read_sql_query(stmt, conn)
            if self.fit_id in df['fit_id'].values:
                logger.info(f"fit_id {self.fit_id} already exists, updating")
                stmt = text("""
                    UPDATE doctrine_fits SET doctrine_name = :doctrine_name,
                    fit_name = :fit_name, ship_type_id = :ship_type_id, ship_name = :ship_name, doctrine_id = :doctrine_id
                    WHERE fit_id = :fit_id
                """)
                conn.execute(stmt, {
                    "doctrine_name": self.doctrine_name,
                    "fit_name": self.fit_name,
                    "ship_type_id": self.ship_type_id,
                    "ship_name": self.ship_name,
                    "doctrine_id": self.doctrine_id,
                    "fit_id": self.fit_id,
                })
                conn.commit()
            else:
                logger.info(f"fit_id {self.fit_id} does not exist, adding")
                stmt = text("""
                    INSERT INTO doctrine_fits (doctrine_name, fit_name, ship_type_id, doctrine_id, fit_id, ship_name)
                    VALUES (:doctrine_name, :fit_name, :ship_type_id, :doctrine_id, :fit_id, :ship_name)
                """)
                conn.execute(stmt, {
                    "doctrine_name": self.doctrine_name,
                    "fit_name": self.fit_name,
                    "ship_type_id": self.ship_type_id,
                    "doctrine_id": self.doctrine_id,
                    "fit_id": self.fit_id,
                    "ship_name": self.ship_name,
                })
                conn.commit()

def convert_fit_date(date: str) -> datetime:
    dt = datetime.strptime("15 Jan 2025 19:12:04", "%d %b %Y %H:%M:%S")
    return dt


def slot_yielder() -> Generator[str, None, None]:
    corrected_order = ['LoSlot', 'MedSlot', 'HiSlot', 'RigSlot', 'DroneBay']
    for slot in corrected_order:
        yield slot
    while True:
        yield 'Cargo'


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
    db = DatabaseConfig("fittings")
    engine = db.remote_engine if remote else db.engine

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
            flag, quantity, type_id, fit_id, type_fk_id = item
            conn.execute(insert_stmt, {
                "flag": flag,
                "quantity": quantity,
                "type_id": type_id,
                "fit_id": fit_id,
                "type_fk_id": type_fk_id
            })

        conn.commit()

        # Re-enable foreign key constraints
        conn.execute(text("PRAGMA foreign_keys = ON"))

        logger.info(f"Inserted {len(fit_items)} items for fit_id {fit_id}")

    engine.dispose()


if __name__ == "__main__":
    pass



