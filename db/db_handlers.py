
import sys
import os
# Add the project root to Python path for direct execution
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from sqlalchemy import select, insert, func, or_
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.sqlite import insert as sqlite_insert  # libSQL/SQLite
from utils.utils import add_timestamp, add_autoincrement, validate_columns
from dotenv import load_dotenv
from config.logging_config import configure_logging
from db.models import Base, MarketHistory,MarketOrders, MarketStats, RegionOrders

from config.config import DatabaseConfig

from utils.utils import add_timestamp, add_autoincrement, validate_columns, convert_datetime_columns, get_type_names
from datetime import datetime, timezone
from db.db_queries import get_table_length, get_remote_status
from esi.esi_requests import fetch_region_orders
import time

load_dotenv()
logger = configure_logging(__name__)

db = DatabaseConfig("wcmkt")
sde_db = DatabaseConfig("sde")


def upsert_remote_database(table: Base, df: pd.DataFrame)->bool:
    #special handling for derived tables like MarketStats that should be completely cleared before updating
    WIPE_REPLACE_TABLES = ["marketstats", "doctrines"]
    tabname = table.__tablename__
    is_wipe_replace = tabname in WIPE_REPLACE_TABLES
    logger.info(f"Processing table: {tabname}, wipe_replace: {is_wipe_replace}")
    #refactored to use upsert instead of insert
    logger.info(f"Upserting {len(df)} rows into {table.__tablename__}")
    data = df.to_dict(orient="records")

    MAX_PARAMETER_BYTES = 256 * 1024  # 256 KB
    BYTES_PER_PARAMETER = 8
    MAX_PARAMETERS = MAX_PARAMETER_BYTES // BYTES_PER_PARAMETER  # 32,768

    column_count = len(df.columns)
    chunk_size = min(2000, MAX_PARAMETERS // column_count)

    logger.info(f"Table {table.__tablename__} has {column_count} columns, using chunk size {chunk_size}")

    db = DatabaseConfig("wcmkt")
    print(f"db: {db}")
    logger.info(f"updating: {db}")

    remote_engine = db.remote_engine
    session = Session(bind=remote_engine)

    t = table.__table__
    pk_cols = list(t.primary_key.columns)
    if len(pk_cols) != 1:
        raise ValueError("This helper expects a single-column primary key.")
    pk_col = pk_cols[0]

    try:
        logger.info(f"Upserting {len(data)} rows into {table.__tablename__}")
        with session.begin():
            if is_wipe_replace:
                logger.info(f"Wiping and replacing {len(data)} rows into {table.__tablename__}")
                # --- Wipe & replace for derived tables (MarketStats, etc.) ---
                session.query(table).delete()
                logger.info(f"Wiped data from {table.__tablename__}")

                for idx in range(0, len(data), chunk_size):
                    chunk = data[idx: idx + chunk_size]
                    stmt = insert(t).values(chunk)  # generic INSERT
                    session.execute(stmt)
                    logger.info(f"  • chunk {idx // chunk_size + 1}, {len(chunk)} rows")

                # Verify exact row count match
                count = session.execute(select(func.count()).select_from(t)).scalar_one()
                if count != len(data):
                    raise RuntimeError(f"Row count mismatch: expected {len(data)}, got {count}")
            else:
                non_pk_cols = [c for c in t.columns if c not in pk_cols]

                for idx in range(0, len(data), chunk_size):
                    chunk = data[idx: idx + chunk_size]
                    base = sqlite_insert(t).values(chunk)
                    excluded = base.excluded  # EXCLUDED pseudo-table
                    # map: {"colname": EXCLUDED.colname, ...} for non-PK
                    set_mapping = {c.name: excluded[c.name] for c in non_pk_cols}
                    changed_pred = or_(*[c.is_distinct_from(excluded[c.name]) for c in non_pk_cols])

                    stmt = base.on_conflict_do_update(
                        index_elements=[pk_col],     # or [pk_col.name]
                        set_=set_mapping,
                        where=changed_pred           # only update if any value actually changed
                    )
                    session.execute(stmt)
                    logger.info(f"  • chunk {idx // chunk_size + 1}, {len(chunk)} rows")

            # sanity check: number of distinct PKs in incoming data <= table rowcount
            distinct_incoming = len({row[pk_col.name] for row in data})
            logger.info(f"distinct incoming: {distinct_incoming}")
            count = session.execute(select(func.count()).select_from(t)).scalar_one()
            logger.info(f"count: {count}")
            if count < distinct_incoming:
                logger.error(f"Row count too low: expected at least {distinct_incoming} unique {pk_col.name}s, got {count}")
                raise RuntimeError(
                    f"Row count too low: expected at least {distinct_incoming} unique {pk_col.name}s, got {count}"
                )

        logger.info(f"Upsert complete: {count} rows present in {table.__tablename__}")
        return True

    except SQLAlchemyError as e:
        logger.error("Failed upserting remote DB", exc_info=e)
        raise e
    finally:
        session.close()
        remote_engine.dispose()

def get_market_history(type_id: int) -> pd.DataFrame:
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        stmt = "SELECT * FROM market_history WHERE type_id = ?"
        result = conn.execute(stmt, (type_id,))
        headers = [col[0] for col in result.description]
    conn.close()
    return pd.DataFrame(result.fetchall(), columns=headers)


def update_history(history_results: list[list[dict]]):
    """
    Process history data from async_history results.
    Each element in history_results corresponds to a type_id and contains
    an array of history records for that type_id.
    """
    valid_history_columns = MarketHistory.__table__.columns.keys()

    # Get the type_ids that were processed (in same order as results)
    watchlist = DatabaseConfig("wcmkt").get_watchlist()
    type_ids = watchlist["type_id"].unique().tolist()

    # Flatten the nested array structure while preserving type_id association
    flattened_history = []
    for i, type_history in enumerate(history_results):
        if i >= len(type_ids):
            logger.warning(f"More history results than type_ids, skipping index {i}")
            continue

        type_id = type_ids[i]
        if isinstance(type_history, list):
            for record in type_history:
                # Add type_id to each record
                record['type_id'] = str(type_id)
                flattened_history.append(record)
        else:
            # Handle case where type_history is not a list
            type_history['type_id'] = str(type_id)
            flattened_history.append(type_history)

    if not flattened_history:
        logger.error("No history data to process")
        return False

    history_df = pd.DataFrame.from_records(flattened_history)

    # Check what columns we actually have
    logger.info(f"Available columns: {list(history_df.columns)}")
    logger.info(f"Expected columns: {list(valid_history_columns)}")

    # Add type_name column by looking up type_id
    from utils.utils import get_type_name
    history_df['type_name'] = history_df['type_id'].apply(lambda x: get_type_name(int(x)))

    # Check if we have the required columns
    missing_columns = set(valid_history_columns) - set(history_df.columns)
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        # Add missing columns with default values
        for col in missing_columns:
            if col == 'id':
                continue  # This will be added by add_autoincrement
            elif col == 'timestamp':
                continue  # This will be added by add_timestamp
            else:
                history_df[col] = 0

    history_df = add_timestamp(history_df)
    history_df = add_autoincrement(history_df)

    history_df = validate_columns(history_df, valid_history_columns)
    history_df = convert_datetime_columns(history_df,['date'])

    history_df.infer_objects()
    history_df.fillna(0)

    try:
        upsert_remote_database(MarketHistory, history_df)
    except Exception as e:
        logger.error(f"history data update failed: {e}")
        return False

    status = get_remote_status()['market_history']
    if status > 0:
        logger.info(f"History updated:{get_table_length('market_history')} items")
        print(f"History updated:{get_table_length('market_history')} items")
    else:
        logger.error(f"Failed to update market history")
        return False
    return True

def update_market_orders(orders: list[dict])->bool:

        orders_df = pd.DataFrame.from_records(orders)
        type_names = get_type_names(orders_df)
        orders_df = orders_df.merge(type_names, on="type_id", how="left")


        # Convert datetime string fields to Python datetime objects for SQLite
        orders_df = convert_datetime_columns(orders_df, ['issued'])
        orders_df = add_timestamp(orders_df)

        orders_df = orders_df.infer_objects()
        orders_df = orders_df.fillna(0)
        print(f"orders_df.isna().sum(): {orders_df.isna().sum()}")

        orders_df = add_autoincrement(orders_df)

        valid_columns = MarketOrders.__table__.columns.keys()
        orders_df = validate_columns(orders_df, valid_columns)

        print(f"Orders fetched:{len(orders_df)} items")
        logger.info(f"Orders fetched:{len(orders_df)} items")

        status = upsert_remote_database(MarketOrders, orders_df)
        if status:
            logger.info(f"Orders updated:{get_table_length('marketorders')} items")
            return True
        else:
            logger.error("Failed to update market orders")
            return False


def update_region_orders(region_id: int, order_type: str = 'sell') -> pd.DataFrame:
    """
    Fetch region orders from the database
    Args:
        region_id: int
        order_type: str (sell, buy, all)
    Returns:
        pandas DataFrame
    """
    orders = fetch_region_orders(region_id, order_type)
    engine = DatabaseConfig("wcmkt").engine
    session = Session(bind=engine)

    # Clear existing orders
    session.query(RegionOrders).delete()
    session.commit()
    session.expunge_all()  # Clear all objects from identity map
    session.close()
    time.sleep(1)
    session = Session(bind=engine)  # Create a fresh session

    # Convert API response dicts to RegionOrders model instances
    for order_data in orders:
        # Convert the API response to match our model fields
        region_order = RegionOrders(
            order_id=order_data['order_id'],
            duration=order_data['duration'],
            is_buy_order=order_data['is_buy_order'],
            issued=datetime.fromisoformat(order_data['issued'].replace('Z', '+00:00')),
            location_id=order_data['location_id'],
            min_volume=order_data['min_volume'],
            price=order_data['price'],
            range=order_data['range'],
            system_id=order_data['system_id'],
            type_id=order_data['type_id'],
            volume_remain=order_data['volume_remain'],
            volume_total=order_data['volume_total']
        )
        session.add(region_order)

    session.commit()
    session.close()

    return pd.DataFrame(orders)
if __name__ == "__main__":
    pass