import pandas as pd
from sqlalchemy import select, insert, func, or_
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from dotenv import load_dotenv
from datetime import datetime
import time

from mkts_backend.utils.utils import (
    add_timestamp,
    add_autoincrement,
    validate_columns,
    convert_datetime_columns,
    get_type_names_from_df,
)
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.db.models import Base, MarketHistory, MarketOrders, RegionOrders
from mkts_backend.config.config import DatabaseConfig
from mkts_backend.db.db_queries import get_table_length, get_remote_status
from mkts_backend.esi.esi_requests import fetch_region_orders

load_dotenv()
logger = configure_logging(__name__)

db = DatabaseConfig("wcmkt")
sde_db = DatabaseConfig("sde")


def upsert_database(table: Base, df: pd.DataFrame) -> bool:
    WIPE_REPLACE_TABLES = ["marketstats", "doctrines"]
    tabname = table.__tablename__
    is_wipe_replace = tabname in WIPE_REPLACE_TABLES
    logger.info(f"Processing table: {tabname}, wipe_replace: {is_wipe_replace}")
    logger.info(f"Upserting {len(df)} rows into {table.__tablename__}")
    data = df.to_dict(orient="records")

    MAX_PARAMETER_BYTES = 256 * 1024
    BYTES_PER_PARAMETER = 8
    MAX_PARAMETERS = MAX_PARAMETER_BYTES // BYTES_PER_PARAMETER

    column_count = len(df.columns)
    chunk_size = min(2000, MAX_PARAMETERS // column_count)

    logger.info(
        f"Table {table.__tablename__} has {column_count} columns, using chunk size {chunk_size}"
    )

    db = DatabaseConfig("wcmkt")
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
            is_wipe_replace = True
            if is_wipe_replace:
                logger.info(
                    f"Wiping and replacing {len(data)} rows into {table.__tablename__}"
                )
                session.query(table).delete()
                logger.info(f"Wiped data from {table.__tablename__}")

                for idx in range(0, len(data), chunk_size):
                    chunk = data[idx : idx + chunk_size]
                    stmt = insert(t).values(chunk)
                    session.execute(stmt)
                    logger.info(
                        f"  • chunk {idx // chunk_size + 1}, {len(chunk)} rows"
                    )

                count = session.execute(select(func.count()).select_from(t)).scalar_one()
                if count != len(data):
                    raise RuntimeError(
                        f"Row count mismatch: expected {len(data)}, got {count}"
                    )
            else:
                non_pk_cols = [c for c in t.columns if c not in pk_cols]

                for idx in range(0, len(data), chunk_size):
                    chunk = data[idx : idx + chunk_size]
                    base = sqlite_insert(t).values(chunk)
                    excluded = base.excluded
                    set_mapping = {c.name: excluded[c.name] for c in non_pk_cols}
                    changed_pred = or_(*[c.is_distinct_from(excluded[c.name]) for c in non_pk_cols])

                    stmt = base.on_conflict_do_update(
                        index_elements=[pk_col], set_=set_mapping, where=changed_pred
                    )
                    session.execute(stmt)
                    logger.info(
                        f"  • chunk {idx // chunk_size + 1}, {len(chunk)} rows"
                    )

            distinct_incoming = len({row[pk_col.name] for row in data})
            logger.info(f"distinct incoming: {distinct_incoming}")
            count = session.execute(select(func.count()).select_from(t)).scalar_one()
            logger.info(f"count: {count}")
            if count < distinct_incoming:
                logger.error(
                    f"Row count too low: expected at least {distinct_incoming} unique {pk_col.name}s, got {count}"
                )
                raise RuntimeError(
                    f"Row count too low: expected at least {distinct_incoming} unique {pk_col.name}s, got {count}"
                )

        logger.info(f"Upsert complete: {count} rows present in {table.__tablename__}")

    except SQLAlchemyError as e:
        logger.error("Failed upserting remote DB", exc_info=e)
        raise e
    finally:
        session.close()
        remote_engine.dispose()
    return True

def update_history(history_results: list[list[dict]]):
    valid_history_columns = MarketHistory.__table__.columns.keys()

    watchlist = DatabaseConfig("wcmkt").get_watchlist()
    type_ids = watchlist["type_id"].unique().tolist()

    flattened_history = []
    for i, type_history in enumerate(history_results):
        if i >= len(type_ids):
            logger.warning(f"More history results than type_ids, skipping index {i}")
            continue

        type_id = type_ids[i]
        if isinstance(type_history, list):
            for record in type_history:
                record['type_id'] = str(type_id)
                flattened_history.append(record)
        else:
            type_history['type_id'] = str(type_id)
            flattened_history.append(type_history)

    if not flattened_history:
        logger.error("No history data to process")
        return False

    history_df = pd.DataFrame.from_records(flattened_history)
    logger.info(f"Available columns: {list(history_df.columns)}")
    logger.info(f"Expected columns: {list(valid_history_columns)}")

    from mkts_backend.utils.utils import get_type_name
    history_df['type_name'] = history_df['type_id'].apply(lambda x: get_type_name(int(x)))

    missing_columns = set(valid_history_columns) - set(history_df.columns)
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        for col in missing_columns:
            if col in ('id', 'timestamp'):
                continue
            else:
                history_df[col] = 0

    history_df = add_timestamp(history_df)
    history_df = add_autoincrement(history_df)
    history_df = validate_columns(history_df, valid_history_columns)
    history_df = convert_datetime_columns(history_df, ['date'])
    history_df.infer_objects()
    history_df.fillna(0)

    try:
        upsert_database(MarketHistory, history_df)
    except Exception as e:
        logger.error(f"history data update failed: {e}")
        return False

    status = get_remote_status()['market_history']
    if status > 0:
        logger.info(f"History updated:{get_table_length('market_history')} items")
        print(f"History updated:{get_table_length('market_history')} items")
    else:
        logger.error("Failed to update market history")
        return False
    return True


def update_market_orders(orders: list[dict]) -> bool:
    orders_df = pd.DataFrame.from_records(orders)
    type_names = get_type_names_from_df(orders_df)
    orders_df = orders_df.merge(type_names, on="type_id", how="left")

    orders_df = convert_datetime_columns(orders_df, ['issued'])
    orders_df = add_timestamp(orders_df)
    orders_df = orders_df.infer_objects()
    orders_df = orders_df.fillna(0)
    orders_df = add_autoincrement(orders_df)

    valid_columns = MarketOrders.__table__.columns.keys()
    orders_df = validate_columns(orders_df, valid_columns)

    logger.info(f"Orders fetched:{len(orders_df)} items")
    status = upsert_database(MarketOrders, orders_df)
    if status:
        logger.info(f"Orders updated:{get_table_length('marketorders')} items")
        return True
    else:
        logger.error("Failed to update market orders")
        return False


def update_region_orders(region_id: int, order_type: str = 'sell') -> pd.DataFrame:
    orders = fetch_region_orders(region_id, order_type)
    engine = DatabaseConfig("wcmkt").engine
    session = Session(bind=engine)

    session.query(RegionOrders).delete()
    session.commit()
    session.expunge_all()
    session.close()
    time.sleep(1)
    session = Session(bind=engine)

    for order_data in orders:
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