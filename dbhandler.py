
import json
import pandas as pd
from sqlalchemy import text, select, insert, func, or_
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.sqlite import insert as sqlite_insert  # libSQL/SQLite
from utils import get_type_name
from dotenv import load_dotenv
from logging_config import configure_logging
from models import Base, MarketHistory,MarketOrders

from dataclasses import dataclass, field
from config import DatabaseConfig

from utils import add_timestamp, add_autoincrement, validate_columns, convert_datetime_columns, get_type_names

load_dotenv()
logger = configure_logging(__name__)

# wcmkt_path = wcmkt_db_path
# wcmkt_local_url = wcmkt_local_url
# sde_local_path = sde_local_path
# sde_local_url = sde_local_url
# wcfittings_db_path = wcfittings_db_path
# fittings_local_url = wc_fittings_local_db_url

db = DatabaseConfig("wcmkt3")
wcmkt_path = db.path
wcmkt_local_url = db.url
wcmkt_turso_url = db.turso_url
wcmkt_turso_token = db.token

sde_db = DatabaseConfig("sde")
sde_path = sde_db.path
sde_local_url = sde_db.url
sde_turso_url = sde_db.turso_url
sde_turso_token = sde_db.token

@dataclass
class TypeInfo:
    type_id: int
    type_name: str = field(init=False)
    group_name: str = field(init=False)
    category_name: str = field(init=False)
    category_id: int = field(init=False)
    group_id: int = field(init=False)
    volume: int = field(init=False)
    def __post_init__(self):
        self.get_type_info()

    def get_type_info(self):
        db = DatabaseConfig("sde")
        stmt = text("SELECT * FROM inv_info WHERE typeID = :type_id")
        engine = db.engine
        with engine.connect() as conn:
            result = conn.execute(stmt, {"type_id": self.type_id})
            for row in result:
                self.type_name = row.typeName
                self.group_name = row.groupName
                self.category_name = row.categoryName
                self.category_id = row.categoryID
                self.group_id = row.groupID
                self.volume = row.volume
        engine.dispose()


def insert_type_data(data: list[dict]):
    db = DatabaseConfig("sde")
    engine = db.engine
    unprocessed_data = []

    with engine.connect() as conn:
        for row in data:
            try:
                type_id = row["type_id"]
                if type_id is None:
                    logger.warning("Type ID is None, skipping...")
                    continue
                logger.info(f"Inserting type data for {row['type_id']}")

                params = (type_id,)

                query = "SELECT typeName FROM Joined_InvTypes WHERE typeID = ?"
                result = conn.execute(query, params)
                try:
                    type_name = result.fetchone()[0]
                except Exception as e:
                    logger.error(f"Error fetching type name: {e}")
                    unprocessed_data.append(row)
                    continue

                row["type_name"] = str(type_name)
            except Exception as e:
                logger.error(f"Error inserting type data: {e}")
                data.remove(row)
                logger.info(f"Removed row: {row}")
    if unprocessed_data:
        logger.info(f"Unprocessed data: {unprocessed_data}")
        with open("unprocessed_data.json", "w") as f:
            json.dump(unprocessed_data, f)
    return data

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

    db = DatabaseConfig("wcmkt3")
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
    db = DatabaseConfig("wcmkt3")
    engine = db.engine
    with engine.connect() as conn:
        stmt = "SELECT * FROM market_history WHERE type_id = ?"
        result = conn.execute(stmt, (type_id,))
        headers = [col[0] for col in result.description]
    conn.close()
    return pd.DataFrame(result.fetchall(), columns=headers)

def get_table_length(table: str) -> int:
    db = DatabaseConfig("wcmkt3")
    engine = db.engine
    with engine.connect() as conn:
        stmt = text(f"SELECT COUNT(*) FROM {table}")
        result = conn.execute(stmt)
        return result.fetchone()[0]

def get_remote_table_list():
    db = DatabaseConfig("wcmkt3")
    remote_tables = db.get_table_list()
    return remote_tables

def get_remote_status():
    db = DatabaseConfig("wcmkt3")
    status_dict = db.get_status()
    return status_dict

def get_watchlist_ids():
    stmt = text("SELECT DISTINCT type_id FROM watchlist")
    db = DatabaseConfig("wcmkt3")
    engine = db.engine
    with engine.connect() as conn:
        result = conn.execute(stmt)
        watchlist_ids = [row[0] for row in result]
    conn.close()
    engine.dispose()
    return watchlist_ids

def get_fit_items(fit_id: int):
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

def add_doctrine_type_info_to_watchlist(doctrine_id: int):
    watchlist_ids = get_watchlist_ids()
    fit_ids = get_fit_ids(doctrine_id)

    missing_fit_items = []

    for fit_id in fit_ids:
        fit_items = get_fit_items(fit_id)
        for item in fit_items:
            if item not in watchlist_ids:
                missing_fit_items.append(item)

    missing_type_info = []

    for item in missing_fit_items:
        stmt4 = text("SELECT * FROM inv_info WHERE typeID = :item")
        db = DatabaseConfig("sde")
        engine = db.engine
        with engine.connect() as conn:
            result = conn.execute(stmt4, {"item": item})
            for row in result:
                type_info = TypeInfo(type_id=item)
                missing_type_info.append(type_info)

    for type_info in missing_type_info:
        stmt5 = text("INSERT INTO watchlist (type_id, type_name, group_name, category_name, category_id, group_id) VALUES (:type_id, :type_name, :group_name, :category_name, :category_id, :group_id)")
        db = DatabaseConfig("wcmkt3")
        engine = db.engine
        with engine.connect() as conn:
            conn.execute(stmt5, {"type_id": type_info.type_id, "type_name": type_info.type_name, "group_name": type_info.group_name, "category_name": type_info.category_name, "category_id": type_info.category_id, "group_id": type_info.group_id})
            conn.commit()
        conn.close()
        engine.dispose()
        logger.info(f"Added {type_info.type_name} to watchlist")
        print(f"Added {type_info.type_name} to watchlist")

def update_history(history: list[dict]):
    valid_history_columns = MarketHistory.__table__.columns.keys()
    history_df = pd.DataFrame.from_records(history)
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


if __name__ == "__main__":
    pass