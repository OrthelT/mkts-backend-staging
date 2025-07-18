import os
import re
from dataclasses import dataclass, field
from typing import Optional, Generator
from collections import defaultdict
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text, insert, select, MetaData, Table
import libsql

from proj_config import fittings_url, sde_url, wcmkt_url
from logging_config import configure_logging

mkt_db = wcmkt_url
sde_db = sde_url
fittings_db = fittings_url

logger = configure_logging(__name__)

@dataclass
class FittingItem:
    flag: str
    quantity: int
    fit_id: int
    type_name: str
    ship_type_name: str
    fit_name: Optional[str] = None

    # Declare attributes that will be assigned in __post_init__
    type_id: int = field(init=False)
    type_fk_id: int = field(init=False)

    def __post_init__(self) -> None:
        self.type_id = self.get_type_id()
        self.type_fk_id = self.type_id  # optional alias
        self.details = self.get_fitting_details()
        if "description" in self.details:
            self.description = self.details['description']
        else:
            self.description = "No description"


        # Only set it if it's not already passed from EFT
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
        engine = create_engine(sde_db, echo=False)
        query = text("SELECT typeID FROM inv_info WHERE typeName = :type_name")
        with engine.connect() as conn:
            result = conn.execute(query, {"type_name": self.type_name}).fetchone()
            return result[0] if result else -1  # return a sentinel or raise error

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
        engine = create_engine(fittings_db)
        with engine.connect() as conn:
            stmt = text("SELECT * FROM fittings_doctrine WHERE id = :doctrine_id")
            result = conn.execute(stmt, {"doctrine_id": self.doctrine_id})
            name = result.fetchone()[1]

            return name.strip()

    def get_ship_type_id(self):
        engine = create_engine(fittings_db)
        with engine.connect() as conn:
            stmt = text("SELECT * FROM fittings_fitting WHERE id = :fit_id")
            result = conn.execute(stmt, {"fit_id": self.fit_id})
            type_id = result.fetchone()[4]
            return type_id

    def get_fit_name(self):
        engine = create_engine(fittings_db)
        with engine.connect() as conn:
            stmt = text("SELECT * FROM fittings_fitting WHERE id = :fit_id")
            result = conn.execute(stmt, {"fit_id": self.fit_id})
            name = result.fetchone()[2]
            return name.strip()

    def get_ship_name(self):
        engine = create_engine(sde_url)
        with engine.connect() as conn:
            stmt = text("SELECT * FROM inv_info WHERE typeID = :type_id")
            result = conn.execute(stmt, {"type_id": self.ship_type_id})
            name = result.fetchone()[1]
            return name.strip()
        
    def add_wcmkts2_doctrine_fits(self):
        engine = create_engine(wcmkt_url)
        with engine.connect() as conn:
            stmt = text("INSERT INTO doctrine_fits (doctrine_name, fit_name, ship_type_id, doctrine_id, fit_id, ship_name, target) VALUES (:doctrine_name, :fit_name, :ship_type_id, :doctrine_id, :fit_id, :ship_name, :target)")
            conn.execute(stmt, {"doctrine_name": self.doctrine_name, "fit_name": self.fit_name, "ship_type_id": self.ship_type_id, "doctrine_id": self.doctrine_id, "fit_id": self.fit_id, "ship_name": self.ship_name, "target": self.target})
            conn.commit()
            print(f"Added doctrine_fits row for doctrine_id={self.doctrine_id}, fit_id={self.fit_id}, target={self.target}")
@dataclass
class WatchDoctrine:
    id: int
    name: str = field(init=False)
    icon_url: str = field(init=False)
    description: str = field(init=False)
    created: str = field(init=False)
    last_updated: str = field(init=False)

    def __post_init__(self):
        self.name = self.get_name()
        self.icon_url = self.get_icon_url()
        self.description = self.get_description()
        self.created = self.get_created()
        self.last_updated = self.get_last_updated()

    def get_name(self):
        engine = create_engine(fittings_db)
        with engine.connect() as conn:
            stmt = text("SELECT * FROM fittings_doctrine WHERE id = :id")
            result = conn.execute(stmt, {"id": self.id})
            name = result.fetchone()[1]
            return name.strip()
    def get_icon_url(self):
        engine = create_engine(fittings_db)
        with engine.connect() as conn:
            stmt = text("SELECT * FROM fittings_doctrine WHERE id = :id")
            result = conn.execute(stmt, {"id": self.id})
            icon_url = result.fetchone()[2]
            return icon_url.strip()

    def get_description(self):
        engine = create_engine(fittings_db)
        with engine.connect() as conn:
            stmt = text("SELECT * FROM fittings_doctrine WHERE id = :id")
            result = conn.execute(stmt, {"id": self.id})
            description = result.fetchone()[3]
            return description.strip()
    def get_created(self):
        engine = create_engine(fittings_db)
        with engine.connect() as conn:
            stmt = text("SELECT * FROM fittings_doctrine WHERE id = :id")
            result = conn.execute(stmt, {"id": self.id})
            created = result.fetchone()[4]
            return created
    def get_last_updated(self):
        engine = create_engine(fittings_db)
        with engine.connect() as conn:
            stmt = text("SELECT * FROM fittings_doctrine WHERE id = :id")
            result = conn.execute(stmt, {"id": self.id})
            last_updated = result.fetchone()[5]
            return last_updated

#Utility functions
def convert_fit_date(date: str) -> datetime:
    """enter date from WC Auth in format: dd Mon YYYY HH:MM:SS
        Example: 15 Jan 2025 19:12:04
    """
    dt = datetime.strptime("15 Jan 2025 19:12:04", "%d %b %Y %H:%M:%S")
    return dt

def slot_yielder() -> Generator[str, None, None]:
    """
    Yields EFT slot flags in correct order.
    Once primary sections are consumed, defaults to 'Cargo'.
    """
    corrected_order = ['LoSlot', 'MedSlot', 'HiSlot', 'RigSlot', 'DroneBay']
    for slot in corrected_order:
        yield slot
    while True:
        yield 'Cargo'

#EFT Fitting Parser
def process_fit(fit_file: str, fit_id: int):
    """
    pass in the path to an EFT formatted fitting file and a fit_id. Returns the fitting items as
    in a list suitable for updating the database.

    :param fit_file: (EFT format)
    :param fit_id: int

    Usage: fit_items = process_fit("drake2501_39.txt", fit_id=39)

    """

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
                # Only advance to the next slot when a blank line *after* content is found
                current_slot = next(slot_gen)
                continue

            if current_slot is None:
                # First block: assign the first slot only when we encounter the first item
                current_slot = next(slot_gen)

            # Parse quantity
            qty_match = re.search(r'\s+x(\d+)$', line)
            if qty_match:
                qty = int(qty_match.group(1))
                item = line[:qty_match.start()].strip()
            else:
                qty = 1
                item = line.strip()

            # Construct slot name
            if current_slot in {'LoSlot', 'MedSlot', 'HiSlot', 'RigSlot'}:
                suffix = slot_counters[current_slot]
                slot_counters[current_slot] += 1
                slot_name = f"{current_slot}{suffix}"
            else:
                slot_name = current_slot  # 'DroneBay' or 'Cargo'

            fitting_item = FittingItem(
                flag=slot_name,
                fit_id=fit_id,
                type_name=item,
                ship_type_name=ship_name,
                fit_name=fit_name,
                quantity=qty
            )

            fit.append([fitting_item.flag, fitting_item.quantity,fitting_item.type_id,fit_id,fitting_item.type_id])

    fitdf = pd.DataFrame(fit, columns=['flag', 'quantity', 'type_id', 'fit_id', 'type_fk_id'])
    print(fitdf)
    pd.set_option('display.max_columns', None)

    confirm = input("Fit look ok? (Y to continue)")
    if confirm == "Y":
        insert_fittings_fittingitems(fitdf)
        print("fitting items inserted")
    else:
        print("fit not inserted, exiting")

def insert_fittings_fittingitems(df: pd.DataFrame):
 
    engine = create_engine(fittings_db, echo=False)
    with engine.connect() as conn:
        conn.execute(text("PRAGMA foreign_keys = OFF"))
        df.to_sql('fittings_fittingitem', conn, if_exists='append', index=False)
        conn.commit()

    print(f"Inserted {len(df)} fitting items")
    print("please use additional functions to complete processing of new fit")

def update_fitting_type(type_id: int, radius: int, packaged_volume: int):
    """
    Populate fittings_type from inv_info (SDE), ensuring its parent
    group and category exist in destination, manually setting radius
    and packaged_volume.

    :param type_id: the typeID to copy from inv_info
    :param radius: manual radius value
    :param packaged_volume: manual packaged_volume value
    """
    # Create engines
    src_engine = create_engine(sde_db)
    dst_engine = create_engine(fittings_db)

    # Reflect source tables
    src_meta = MetaData()
    inv_info = Table("inv_info", src_meta, autoload_with=src_engine)
    # Reflect destination tables
    dst_meta = MetaData()
    fittings_itemcategory = Table("fittings_itemcategory", dst_meta, autoload_with=dst_engine)
    fittings_itemgroup    = Table("fittings_itemgroup",    dst_meta, autoload_with=dst_engine)
    fittings_type_table   = Table("fittings_type",         dst_meta, autoload_with=dst_engine)

    # Fetch source rows
    with src_engine.connect() as src_conn:
        type_row = src_conn.execute(
            select(inv_info).where(inv_info.c.typeID == type_id)
        ).first()
        if not type_row:
            raise ValueError(f"type_id {type_id} not found in inv_info")

    # Prepare insert payloads
    data_category = {
        "category_id": type_row.categoryID,
        # adjust column name if needed (e.g., 'name' vs 'category_name')
        "name":        type_row.categoryName,
    }
    data_group = {
        "group_id":    type_row.groupID,
        "name":        type_row.groupName,
        "category_id": type_row.categoryID,
    }
    data_type = {
        "type_id":         type_row.typeID,
        "type_name":       type_row.typeName,
        "published":       type_row.published,
        "mass":            type_row.mass,
        "capacity":        type_row.capacity,
        "description":     type_row.description,
        "volume":          type_row.volume,
        "packaged_volume": packaged_volume,
        "portion_size":    type_row.portionSize,
        "radius":          radius,
        "graphic_id":      type_row.graphicID,
        "icon_id":         type_row.iconID,
        "market_group_id": type_row.marketGroupID,
        "group_id":        type_row.groupID,
    }

    # Perform upserts in proper order
    with dst_engine.begin() as dst_conn:
        # 1) Upsert category first
        stmt_cat = insert(fittings_itemcategory).values(**data_category)
        stmt_cat = stmt_cat.on_duplicate_key_update(
            name=stmt_cat.inserted.name,
            published=stmt_cat.inserted.published
        )
        dst_conn.execute(stmt_cat)

        # 2) Upsert group next
        stmt_grp = insert(fittings_itemgroup).values(**data_group)
        stmt_grp = stmt_grp.on_duplicate_key_update(
            name=stmt_grp.inserted.name,
            category_id=stmt_grp.inserted.category_id,
            published=stmt_grp.inserted.published
        )
        dst_conn.execute(stmt_grp)

        # 3) Insert type
        dst_conn.execute(insert(fittings_type_table).values(**data_type))

    print(f"Upserted category {type_row.categoryID}, group {type_row.groupID}, "
          f"and inserted type {type_id} (radius={radius}, packaged_volume={packaged_volume})")


def add_new_fitting(fitting_dict: dict):
    """
    Inserts a new row into fittings_fitting using a parameter dict.
    Expected keys in fitting_dict: id, description, name, ship_type_type_id,
    ship_type_id, created, last_updated

    example:

    fittings_dict = {'id': 991, 'description': 'special anti-Kiki Kesteral',
                    'name': 'Anti-Kiki Kesteral', 'ship_type_type_id': 43563,
                    'ship_type_id': 43563, 'created': '2025-05-15 19:26:23.133593',
                    'last_updated': '2025-05-15 19:26:23.133593'}

    """
    stmt = text(
        """
        INSERT INTO fittings_fitting (
            id,
            description,
            name,
            ship_type_type_id,
            ship_type_id,
            created,
            last_updated
        )
        VALUES (
            :id,
            :description,
            :name,
            :ship_type_type_id,
            :ship_type_id,
            :created,
            :last_updated
        )
        """
    )

    engine = create_engine(fittings_db)
    with engine.begin() as conn:
        conn.execute(stmt, fitting_dict)
    print(f"Added fitting with id={fitting_dict['id']} and name={fitting_dict['name']}")

def update_fitting(fitting_id: int, description: str, name: str):
    """
    Update the description and name of a fitting record.

    :param fitting_id:  primary key ID of the fitting to update
    :param description: new description text
    :param name:        new name text

    usage:
        update_fitting(
        fitting_id=991,
        description="special anti-Kiki Kestrel",
        name="Anti-Kiki Kestrel"
        )
    """
    engine = create_engine(fittings_db)
    stmt = text(
        """
        UPDATE fittings_fitting
        SET description = :description,
            name        = :name
        WHERE id       = :id
        """
    )

    # Use a transaction context; commits on exit
    with engine.begin() as conn:
        conn.execute(stmt, {
            "id": fitting_id,
            "description": description,
            "name": name
        })

    print(f"Updated fitting id={fitting_id} successfully")

def change_fitting_id(id, new_fit_id):
    """
    :param id:
    :param new_fit_id:

    usage:
        change_fitting_id(
        id=5225,
        new_fit_id=991
        )
    """
    engine = create_engine(fittings_db)
    stmt = text(
        """
        UPDATE fittings_doctrine_fittings
        SET fitting_id = :new_fit_id
        WHERE id = :id
        """)

    with engine.begin() as conn:
        conn.execute(stmt, {
            "new_fit_id": new_fit_id,
            "id": id
        })
    print(f"Updated fitting fit_id={new_fit_id} successfully")

def check_type_ids(type_ids: list[int])->list[int] | None:
    missing_type_ids = []
    print("checking type_ids")

    for id in type_ids:
        print("checking id:", id)
        engine = create_engine(fittings_db, echo=False)
        query = text("SELECT type_name FROM fittings_type WHERE type_id = :id")
        try:
            with engine.connect() as conn:
                name = conn.execute(query, {"id": id})
                if name.first() is None:
                    print("missing type_id:", id)
                    missing_type_ids.append(id)
                else:
                    print(f"type_id: {id} OK")
        except:
            print(f"Could not find type with id={id}")
            missing_type_ids.append(id)
            continue

    if missing_type_ids:
        ok = [id for id in type_ids if id not in missing_type_ids]
        print("-"*60)
        print("-"*60)
        print("type_ids ok:", ok)
        print("type_ids missing:", missing_type_ids)
        return missing_type_ids
    else:
        print("no type_ids to add")
        return None

if __name__ == '__main__':
    pass