import sys
import os
# Add the project root to Python path for direct execution
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy import select
import pandas as pd
from db.models import RegionOrders
from config.config import DatabaseConfig
from utils.get_type_info import TypeInfo


def get_table_length(table: str) -> int:
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        stmt = text(f"SELECT COUNT(*) FROM {table}")
        result = conn.execute(stmt)
        return result.fetchone()[0]

def get_remote_table_list():
    db = DatabaseConfig("wcmkt")
    remote_tables = db.get_table_list()
    return remote_tables

def get_remote_status():
    db = DatabaseConfig("wcmkt")
    status_dict = db.get_status()
    return status_dict

def get_watchlist_ids():
    stmt = text("SELECT DISTINCT type_id FROM watchlist")
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        result = conn.execute(stmt)
        watchlist_ids = [row[0] for row in result]
    conn.close()
    engine.dispose()
    return watchlist_ids

def get_fit_items(fit_id: int) -> list[int]:
    stmt = text("SELECT type_id FROM fittings_fittingitem WHERE fit_id = :fit_id")
    db = DatabaseConfig("fittings")
    engine = db.engine
    with engine.connect() as conn:
        result = conn.execute(stmt, {"fit_id": fit_id})
        fit_items = [row[0] for row in result]
    conn.close()
    engine.dispose()
    return fit_items

def get_fit_ids(doctrine_id: int):
    stmt = text("SELECT fitting_id FROM fittings_doctrine_fittings WHERE doctrine_id = :doctrine_id")
    db = DatabaseConfig("fittings")
    engine = db.engine
    with engine.connect() as conn:
        result = conn.execute(stmt, {"doctrine_id": doctrine_id})
        fit_ids = [row[0] for row in result]
    conn.close()
    engine.dispose()
    return fit_ids


def get_region_orders_from_db(region_id: int, system_id: int, db: DatabaseConfig) -> pd.DataFrame:
    """
    Get all orders for a given region and order type
    Args:
        region_id: int
        order_type: str (sell, buy, all)
    Returns:
        pandas DataFrame
    """
    stmt = select(RegionOrders).where(RegionOrders.system_id == system_id)

    engine = db.engine
    session = Session(bind=engine)
    result = session.scalars(stmt)
    orders_data = []
    for order in result:
        orders_data.append({
            'order_id': order.order_id,
            'duration': order.duration,
            'is_buy_order': order.is_buy_order,
            'issued': order.issued,
            'location_id': order.location_id,
            'min_volume': order.min_volume,
            'price': order.price,
            'range': order.range,
            'system_id': order.system_id,
            'type_id': order.type_id,
            'volume_remain': order.volume_remain,
            'volume_total': order.volume_total
        })

    session.close()
    return pd.DataFrame(orders_data)

def get_system_orders_from_db(system_id: int) -> pd.DataFrame:
    """
    Get all orders for a given system
    Args:
        system_id: int
    Returns:
        pandas DataFrame
    """
    stmt = select(RegionOrders).where(RegionOrders.system_id == system_id)
    engine = DatabaseConfig("wcmkt2").engine
    session = Session(bind=engine)
    result = session.scalars(stmt)

    # Convert SQLAlchemy objects to dictionaries for DataFrame
    orders_data = []
    for order in result:
        orders_data.append({
            'order_id': order.order_id,
            'duration': order.duration,
            'is_buy_order': order.is_buy_order,
            'issued': order.issued,
            'location_id': order.location_id,
            'min_volume': order.min_volume,
            'price': order.price,
            'range': order.range,
            'system_id': order.system_id,
            'type_id': order.type_id,
            'volume_remain': order.volume_remain,
            'volume_total': order.volume_total
        })

    session.close()
    return pd.DataFrame(orders_data)

def get_region_history()-> pd.DataFrame:
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM region_history")
        result = conn.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    conn.close()
    return df
if __name__ == "__main__":
    items = get_fit_items(494)
    for item in items:
        item_id = item
        item_name = TypeInfo(type_id=item_id)
        type_id = item_name.type_id
        type_name = item_name.type_name
        print(type_id, type_name)