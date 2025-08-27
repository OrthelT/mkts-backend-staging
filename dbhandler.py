
import json
import pandas as pd
from sqlalchemy import text, select, insert, func
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

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
        stmt = sa.text("SELECT * FROM inv_info WHERE typeID = :type_id")
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

def update_remote_database(table: Base, df: pd.DataFrame):
    data = df.to_dict(orient="records")

    MAX_PARAMETER_BYTES = 256 * 1024  # 256 KB
    BYTES_PER_PARAMETER = 8
    MAX_PARAMETERS = MAX_PARAMETER_BYTES // BYTES_PER_PARAMETER  # 32,768

    column_count = len(df.columns)
    chunk_size = min(2000, MAX_PARAMETERS // column_count)

    print(f"Table {table.__tablename__} has {column_count} columns, using chunk size {chunk_size}")

    db = DatabaseConfig("wcmkt3")
    remote_engine = db.remote_engine
    session = Session(bind=remote_engine)

    try:
        print(f"Updating {table.__tablename__} with {len(data)} rows")
        with session.begin():
            # 1. Wipe out old rows
            session.query(table).delete()

            # 2. Insert in chunks
            for idx in range(0, len(data), chunk_size):
                chunk = data[idx: idx + chunk_size]
                stmt  = insert(table).values(chunk)
                session.execute(stmt)
                print(f"  • chunk {idx // chunk_size + 1}, {len(chunk)} rows")

            # 3. Verify the row count
            count_stmt = select(func.count()).select_from(table)
            count = session.execute(count_stmt).scalar_one()
            if count != len(data):
                raise RuntimeError(
                    f"Row count mismatch: expected {len(data)}, got {count}"
                )

        logger.info(f"Database updated successfully: {count} rows in {table.__tablename__}")

    except SQLAlchemyError as e:
        # any SQL error rolls back in the context manager
        logger.error("Failed updating remote DB", exc_info=e)
        return False

    finally:
        session.close()
        remote_engine.dispose()
    return True

def get_watchlist() -> pd.DataFrame:
    db = DatabaseConfig("wcmkt3")
    df = db.get_watchlist()
    return df

def get_nakah_watchlist() -> pd.DataFrame:
    db = DatabaseConfig("wcmkt3_turso")
    engine = db.engine
    with engine.connect() as conn:
        df = pd.read_sql_table("nakah_watchlist", conn)
        if len(df) == 0:
            logger.error("No nakah watchlist found")
            return None
        else:
            print(f"nakah watchlist loaded: {len(df)} items")
            logger.info(f"nakah watchlist loaded: {len(df)} items")
    return df

def update_watchlist_data():
    df = pd.read_csv("data/all_watchlist.csv")
    db = DatabaseConfig("wcmkt3")
    engine = db.engine
    with engine.connect() as conn:
        df.to_sql("watchlist", conn, if_exists="replace", index=False)
        conn.commit()
    conn.close()

def update_nakah_watchlist(df):
    db = DatabaseConfig("wcmkt3")
    engine = db.engine
    with engine.connect() as conn:
        df.to_sql("nakah_watchlist", conn, if_exists="replace", index=False)
        conn.commit()
    engine.dispose()
    print("nakah_watchlist updated")

def get_market_orders(type_id: int) -> pd.DataFrame:
    db = DatabaseConfig("wcmkt3")
    engine = db.engine
    with engine.connect() as conn:
        stmt = "SELECT * FROM marketorders WHERE type_id = ?"
        result = conn.execute(stmt, (type_id,))
        headers = [col[0] for col in result.description]
    conn.close()
    return pd.DataFrame(result.fetchall(), columns=headers)


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

def load_additional_tables():
    df = pd.read_csv("data/doctrine_map.csv")
    targets = pd.read_csv("data/ship_targets.csv")
    db = DatabaseConfig("wcmkt3")
    engine = db.engine
    with engine.connect() as conn:
        df.to_sql("doctrine_map", conn, if_exists="replace", index=False)
        targets.to_sql("ship_targets", conn, if_exists="replace", index=False)
        conn.commit()
    conn.close()

# def sync_db(db_url="wcmkt2.db", sync_url=turso_url, auth_token=turso_auth_token):
#     logger.info("database sync started")
#     # Skip sync in development mode or when sync_url/auth_token are not provided
#     if not sync_url or not auth_token:
#         logger.info(
#             "Skipping database sync in development mode or missing sync credentials"
#         )
#         return

#     try:
#         sync_start = time.time()
#         conn = libsql.connect(db_url, sync_url=sync_url, auth_token=auth_token)
#         logger.info("\n")
#         logger.info("=" * 80)
#         logger.info(f"Database sync started at {sync_start}")
#         try:
#             conn.sync()
#             logger.info(
#                 f"Database synced in {1000 * (time.time() - sync_start)} milliseconds"
#             )
#             print(
#                 f"Database synced in {1000 * (time.time() - sync_start)} milliseconds"
#             )
#         except Exception as e:
#             logger.error(f"Sync failed: {str(e)}")

#         last_sync = datetime.now(timezone.utc)
#         print(last_sync)
#     except Exception as e:
#         if "Sync is not supported" in str(e):
#             logger.info(
#                 "Skipping sync: This appears to be a local file database that doesn't support sync"
#             )
#         else:
#             logger.error(f"Sync failed: {str(e)}")

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


def process_history(history: list[dict]):
    valid_history_columns = MarketHistory.__table__.columns.keys()
    history_df = pd.DataFrame.from_records(history)
    history_df = add_timestamp(history_df)
    history_df = add_autoincrement(history_df)
    history_df = validate_columns(history_df, valid_history_columns)

    history_df = convert_datetime_columns(history_df,['date'])

    history_df.infer_objects()
    history_df.fillna(0)

    try:
        update_remote_database(MarketHistory, history_df)
    except Exception as e:
        logger.error(f"history data update failed: {e}")

    status = get_remote_status()['market_history']
    if status > 0:
        logger.info(f"History updated:{get_table_length('market_history')} items")
        print(f"History updated:{get_table_length('market_history')} items")
    else:
        logger.error(f"Failed to update market history")
        return False
    return True

def process_market_orders(orders: list[dict])->bool:
        valid_columns = MarketOrders.__table__.columns.keys()
        valid_columns = [col for col in valid_columns]
        orders_df = pd.DataFrame.from_records(orders)
        type_names = get_type_names(orders_df)
        orders_df = orders_df.merge(type_names, on="type_id", how="left")
        orders_df = orders_df[valid_columns]
        # Convert datetime string fields to Python datetime objects for SQLite
        orders_df = convert_datetime_columns(orders_df, ['issued'])
        orders_df = add_timestamp(orders_df)

        orders_df = orders_df.infer_objects()
        orders_df = orders_df.fillna(0)

        orders_df = add_autoincrement(orders_df)
        orders_df = validate_columns(orders_df, valid_columns)

        print(f"Orders fetched:{len(orders_df)} items")
        logger.info(f"Orders fetched:{len(orders_df)} items")

        status = update_remote_database(MarketOrders, orders_df)
        if status:
            logger.info(f"Orders updated:{get_table_length('marketorders')} items")
        else:
            logger.error("Failed to update market orders")
        return status

if __name__ == "__main__":
    pass
