from sqlalchemy import text, select
from sqlalchemy.orm import Session
import pandas as pd

from mkts_backend.db.models import RegionOrders
from mkts_backend.config.config import DatabaseConfig
from mkts_backend.utils.get_type_info import TypeInfo

def get_market_history(type_id: int) -> pd.DataFrame:
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        stmt = "SELECT * FROM market_history WHERE type_id = ?"
        result = conn.execute(stmt, (type_id,))
        headers = [col[0] for col in result.description]
    conn.close()
    return pd.DataFrame(result.fetchall(), columns=headers)

def get_market_orders(type_id: int) -> pd.DataFrame:
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        stmt = "SELECT * FROM market_orders WHERE type_id = ?"
        result = conn.execute(stmt, (type_id,))
        headers = [col[0] for col in result.description]
    conn.close()
    return pd.DataFrame(result.fetchall(), columns=headers)

def get_market_stats(type_id: int) -> pd.DataFrame:
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM marketstats WHERE type_id = :type_id")
        df = pd.read_sql_query(stmt, conn, params={"type_id": type_id})
    conn.close()
    return df

def get_doctrine_stats(type_id: int) -> pd.DataFrame:
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM doctrines WHERE type_id = :type_id")
        df = pd.read_sql_query(stmt, conn, params={"type_id": type_id})
    conn.close()
    return df

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
    stmt = select(RegionOrders).where(RegionOrders.system_id == system_id)
    engine = DatabaseConfig("wcmkt2").engine
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


def get_region_history() -> pd.DataFrame:
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM region_history")
        result = conn.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    conn.close()
    return df

if __name__ == "__main__":
    pass