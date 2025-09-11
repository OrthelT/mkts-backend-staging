from utils.db_utils import add_doctrine_type_info_to_watchlist
from utils.get_type_info import TypeInfo
from config.config import DatabaseConfig
from sqlalchemy import text
from utils.utils import get_type_name
from db.models import Doctrines
from dataclasses import dataclass
import pandas as pd
from utils.utils import init_databases
from config.logging_config import configure_logging
doctrines_fields = ['id', 'fit_id', 'ship_id', 'ship_name', 'hulls', 'type_id', 'type_name', 'fit_qty', 'fits_on_mkt', 'total_stock', 'price', 'avg_vol', 'days', 'group_id', 'group_name', 'category_id', 'category name', 'timestamp']
logger = configure_logging(__name__)

@dataclass
class DoctrineFit:
    fit_id: int
    ship_id: int
    ship_name: str
    ship_target: int
    created_at: str

def add_ship_target():
    db = DatabaseConfig("wcmkt")
    stmt = text("""INSERT INTO ship_targets ('fit_id', 'fit_name', 'ship_id', 'ship_name', 'ship_target', 'created_at')
    VALUES (494, '2507  WC-EN Shield DPS HFI v1.0', 33157, 'Hurricane Fleet Issue', 100, '2025-07-05 00:00:00')""")
    engine = db.remote_engine
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()
        print("Ship target added")
    conn.close()
    engine.dispose()

def add_doctrine_map_from_fittings_doctrine_fittings(doctrine_id: int):
    db = DatabaseConfig("fittings")
    engine = db.remote_engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM fittings_doctrine_fittings WHERE doctrine_id = :doctrine_id")
        df = pd.read_sql_query(stmt, conn, params={"doctrine_id": doctrine_id})
        print(df)
    conn.close()
    doctrine_map_db = DatabaseConfig("wcmkt")
    engine = doctrine_map_db.remote_engine
    with engine.connect() as conn:
        for index, row in df.iterrows():
            stmt = text("INSERT INTO doctrine_map ('doctrine_id', 'fitting_id') VALUES (:doctrine_id, :fitting_id)")
            conn.execute(stmt, {"doctrine_id": doctrine_id, "fitting_id": row.fitting_id})
            logger.info(f"Added doctrine_map for doctrine_id: {doctrine_id}, fitting_id: {row.fitting_id}")
    conn.close()
    engine.dispose()


def add_hurricane_fleet_issue_to_doctrines():
    """
    Add Hurricane Fleet Issue (fit_id 494) to the doctrines table with all required fields.
    Uses market data from marketstats table and type info from TypeInfo class.

    Field Logic:
    - hulls: Number of ship hulls on market (for ship_id)
    - total_stock: Total volume of the specific item on market (for type_id)
    - fits_on_mkt: total_stock / fit_qty

    For this ship entry (ship_id == type_id), both hulls and total_stock are the same value.
    For module entries, hulls would be the ship volume and total_stock would be the module volume.
    """
    from datetime import datetime, timezone

    # Get market data for Hurricane Fleet Issue
    db = DatabaseConfig("wcmkt")
    engine = db.remote_engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM marketstats WHERE type_id = 33157")
        market_data = conn.execute(stmt).fetchone()
    conn.close()
    engine.dispose()

    if not market_data:
        logger.error("No market data found for Hurricane Fleet Issue (type_id 33157)")
        return False

    # Get type info
    type_info = TypeInfo(33157)

    # Get the next available ID
    engine = db.remote_engine
    with engine.connect() as conn:
        stmt = text('SELECT MAX(id) as max_id FROM doctrines')
        result = conn.execute(stmt).fetchone()
        max_id = result.max_id if result.max_id else 0
        next_id = max_id + 1
        logger.info(f"Next available ID: {next_id}")
    conn.close()
    engine.dispose()

    # Calculate fits_on_mkt: total_stock / fit_qty
    fit_qty = 1  # As specified in the user's request

    # For the Hurricane Fleet Issue ship itself:
    # - hulls = number of Hurricane Fleet Issue ships on market (ship_id volume)
    # - total_stock = number of Hurricane Fleet Issue ships on market (type_id volume)
    # - Since ship_id == type_id in this case, both are the same value
    hulls_on_market = market_data.total_volume_remain  # Hurricane Fleet Issue ships on market
    total_stock_on_market = market_data.total_volume_remain  # Same value since ship_id == type_id
    fits_on_mkt = total_stock_on_market / fit_qty

    # Prepare the insert statement with all required fields (including manually generated id)
    stmt = text("""
        INSERT INTO doctrines (
            id, fit_id, ship_id, ship_name, hulls, type_id, type_name, fit_qty,
            fits_on_mkt, total_stock, price, avg_vol, days, group_id,
            group_name, category_id, category_name, timestamp
        ) VALUES (
            :id, :fit_id, :ship_id, :ship_name, :hulls, :type_id, :type_name, :fit_qty,
            :fits_on_mkt, :total_stock, :price, :avg_vol, :days, :group_id,
            :group_name, :category_id, :category_name, :timestamp
        )
    """)



    # Prepare the data
    insert_data = {
        'id': next_id,  # Manually generated unique ID
        'fit_id': 494,
        'ship_id': 33157,
        'ship_name': 'Hurricane Fleet Issue',
        'hulls': int(hulls_on_market),  # Number of Hurricane Fleet Issue ships on market
        'type_id': 33157,
        'type_name': type_info.type_name,
        'fit_qty': fit_qty,
        'fits_on_mkt': float(fits_on_mkt),
        'total_stock': int(total_stock_on_market),  # Total volume of Hurricane Fleet Issue ships on market
        'price': float(market_data.price),
        'avg_vol': float(market_data.avg_volume),
        'days': float(market_data.days_remaining),
        'group_id': int(type_info.group_id),
        'group_name': type_info.group_name,
        'category_id': int(type_info.category_id),
        'category_name': type_info.category_name,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

    # Execute the insert
    engine = db.remote_engine
    with engine.connect() as conn:
        conn.execute(stmt, insert_data)
        conn.commit()
        logger.info(f"Successfully added Hurricane Fleet Issue (fit_id 494) to doctrines table")
        print("Hurricane Fleet Issue added to doctrines table successfully!")
    conn.close()
    engine.dispose()

    return True

def add_fit_to_doctrines_table(DoctrineFit: DoctrineFit):
    db = DatabaseConfig("wcmkt")
    columns = db.get_table_columns("doctrines")
    print(columns)
    stmt = text("""INSERT INTO doctrines ('fit_id', 'fit_name', 'ship_id', 'ship_name', 'ship_target', 'created_at')
    VALUES (494, '2507  WC-EN Shield DPS HFI v1.0', 33157, 'Hurricane Fleet Issue', 100, '2025-07-05 00:00:00')""")
    engine = db.remote_engine
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()
        print("Fit added to doctrines table")
    conn.close()
    engine.dispose()

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)

    # Add Hurricane Fleet Issue to doctrines table
    print("Adding Hurricane Fleet Issue to doctrines table...")
    add_hurricane_fleet_issue_to_doctrines()

    # Verify the data was inserted
    print("\nVerifying inserted data:")
    db = DatabaseConfig("wcmkt")
    engine = db.remote_engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM doctrines WHERE fit_id = 494")
        df = pd.read_sql_query(stmt, conn)
        print(df)
    conn.close()
    engine.dispose()
