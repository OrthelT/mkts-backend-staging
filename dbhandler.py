import os
import libsql
import json
import requests
import pandas as pd
from sqlalchemy import inspect, text, create_engine, select, insert, MetaData,func
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
import time
from utils import standby, logger, configure_logging, get_type_name
from dotenv import load_dotenv
from logging_config import configure_logging
from models import Base, Doctrines, RegionHistory, NakahWatchlist, MarketHistory,MarketOrders
from proj_config import wcmkt_db_path, wcmkt_local_url, sde_local_path, sde_local_url, wcfittings_db_path, wcfittings_db_path, wc_fittings_local_db_url
from dataclasses import dataclass, field
import sqlalchemy as sa

load_dotenv()
logger = configure_logging(__name__)

wcmkt_path = wcmkt_db_path
wcmkt_local_url = wcmkt_local_url
sde_local_path = sde_local_path
sde_local_url = sde_local_url
wcfittings_db_path = wcfittings_db_path
fittings_local_url = wc_fittings_local_db_url

turso_url = os.getenv("TURSO_URL")
turso_auth_token = os.getenv("TURSO_AUTH_TOKEN")

test_url = os.getenv("TURSO_TESTING_URL")
test_auth_token = os.getenv("TURSO_TESTING_AUTH_TOKEN")

sde_local_url = os.getenv("SDE_URL")
sde_token = os.getenv("SDE_AUTH_TOKEN")

turso_fittings_url = os.getenv("TURSO_FITTINGS_URL")
turso_fittings_auth_token = os.getenv("TURSO_FITTINGS_AUTH_TOKEN")

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
        stmt = sa.text("SELECT * FROM inv_info WHERE typeID = :type_id")
        engine = sa.create_engine(sde_local_url)
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

# SDE connection
def sde_conn():
    conn = libsql.connect(sde_local_path, sync_url=sde_local_url, auth_token=sde_token)
    return conn

# WCMKT connection
def wcmkt_conn():
    conn = libsql.connect(wcmkt_path, sync_url=turso_url, auth_token=turso_auth_token)
    return conn

def fittings_conn():
    conn = libsql.connect(wcfittings_db_path, sync_url=turso_fittings_url, auth_token=turso_fittings_auth_token)
    return conn

def sde_remote_engine():
    engine = create_engine(sde_local_url, connect_args={"auth_token": sde_token}, echo=True)
    return engine

def sde_local_engine():
    engine = create_engine(sde_local_url)
    return engine

def get_wcmkt_remote_engine():
    engine = create_engine(
    f"sqlite+{turso_url}?secure=true",
    connect_args={
        "auth_token": turso_auth_token,
    },echo=False)
    return engine

def get_wcmkt_local_engine():
        engine = create_engine(wcmkt_local_url, echo=False)
        return engine

def insert_type_data(data: list[dict]):
    conn = libsql.connect(sde_local_path)
    cursor = conn.cursor()
    unprocessed_data = []
    for row in data:
        try:
            type_id = row["type_id"]
            if type_id is None:
                logger.warning("Type ID is None, skipping...")
                continue
            logger.info(f"Inserting type data for {row['type_id']}")

            params = (type_id,)

            query = "SELECT typeName FROM Joined_InvTypes WHERE typeID = ?"
            cursor.execute(query, params)
            try:
                type_name = cursor.fetchone()[0]
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

    # Calculate optimal chunk size based on SQLite memory limits
    # SQLite has ~256KB limit for prepared statements
    # Each parameter takes 8 bytes
    MAX_PARAMETER_BYTES = 256 * 1024  # 256 KB
    BYTES_PER_PARAMETER = 8
    MAX_PARAMETERS = MAX_PARAMETER_BYTES // BYTES_PER_PARAMETER  # 32,768

    column_count = len(df.columns)
    chunk_size = min(2000, MAX_PARAMETERS // column_count)

    print(f"Table {table.__tablename__} has {column_count} columns, using chunk size {chunk_size}")

    remote_engine = get_wcmkt_remote_engine()
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

        # if we get here, commit happened automatically
        logger.info(f"Database updated successfully: {count} rows in {table.__tablename__}")

    except SQLAlchemyError as e:
        # any SQL error rolls back in the context manager
        logger.error("Failed updating remote DB", exc_info=e)
        return False

    finally:
        session.close()
        remote_engine.dispose()

def get_watchlist() -> pd.DataFrame:
    engine = create_engine(wcmkt_local_url)
    with engine.connect() as conn:
        df = pd.read_sql_table("watchlist", conn)
        if len(df) == 0:
            logger.error("No watchlist found")
            update_choice = input("No watchlist found, press Y to update from csv (data/all_watchlist.csv)")
            if update_choice == "Y":
                update_watchlist_data()
                df = pd.read_sql_table("watchlist", conn)
            else:
                logger.error("No watchlist found")
                return None

            if len(df) == 0:
                print("watchlist loading")
                standby(10)
                df = pd.read_sql_table("watchlist", conn)
            if len(df) == 0:
                logger.error("No watchlist found")
                return None
        else:
            print(f"watchlist loaded: {len(df)} items")
            logger.info(f"watchlist loaded: {len(df)} items")
    return df

def get_nakah_watchlist() -> pd.DataFrame:
    engine = create_engine(wcmkt_local_url)
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
    engine = create_engine(wcmkt_local_url)
    with engine.connect() as conn:
        df.to_sql("watchlist", conn, if_exists="replace", index=False)
        conn.commit()
    conn.close()

def update_nakah_watchlist(df):
    engine = create_engine(wcmkt_local_url)
    with engine.connect() as conn:
        df.to_sql("nakah_watchlist", conn, if_exists="replace", index=False)
        conn.commit()
    engine.dispose()
    print("nakah_watchlist updated")

def get_market_orders(type_id: int) -> pd.DataFrame:
    conn = libsql.connect(wcmkt_path)
    cursor = conn.cursor()
    stmt = "SELECT * FROM marketorders WHERE type_id = ?"
    cursor.execute(stmt, (type_id,))
    headers = [col[0] for col in cursor.description]
    conn.close
    return pd.DataFrame(cursor.fetchall(), columns=headers)


def get_market_history(type_id: int) -> pd.DataFrame:
    conn = libsql.connect(wcmkt_path)
    cursor = conn.cursor()
    stmt = "SELECT * FROM market_history WHERE type_id = ?"
    cursor.execute(stmt, (type_id,))
    headers = [col[0] for col in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=headers)


def get_table_length(table: str) -> int:
    conn = libsql.connect(wcmkt_path)
    cursor = conn.cursor()
    stmt = f"SELECT COUNT(*) FROM {table}"
    cursor.execute(stmt)
    return cursor.fetchone()[0]


def load_additional_tables():
    df = pd.read_csv("data/doctrine_map.csv")
    targets = pd.read_csv("data/ship_targets.csv")
    engine = create_engine(wcmkt_local_url)
    with engine.connect() as conn:
        df.to_sql("doctrine_map", conn, if_exists="replace", index=False)
        targets.to_sql("ship_targets", conn, if_exists="replace", index=False)
        conn.commit()

def sync_db(db_url="wcmkt2.db", sync_url=turso_url, auth_token=turso_auth_token):
    logger.info("database sync started")
    # Skip sync in development mode or when sync_url/auth_token are not provided
    if not sync_url or not auth_token:
        logger.info(
            "Skipping database sync in development mode or missing sync credentials"
        )
        return

    try:
        sync_start = time.time()
        conn = libsql.connect(db_url, sync_url=sync_url, auth_token=auth_token)
        logger.info("\n")
        logger.info("=" * 80)
        logger.info(f"Database sync started at {sync_start}")
        try:
            conn.sync()
            logger.info(
                f"Database synced in {1000 * (time.time() - sync_start)} milliseconds"
            )
            print(
                f"Database synced in {1000 * (time.time() - sync_start)} milliseconds"
            )
        except Exception as e:
            logger.error(f"Sync failed: {str(e)}")

        last_sync = datetime.now(timezone.utc)
        print(last_sync)
    except Exception as e:
        if "Sync is not supported" in str(e):
            logger.info(
                "Skipping sync: This appears to be a local file database that doesn't support sync"
            )
        else:
            logger.error(f"Sync failed: {str(e)}")

def get_remote_table_list():
    remote_engine = get_wcmkt_remote_engine()
    with remote_engine.connect() as conn:
        tables = conn.execute(text("PRAGMA table_list"))
        return tables.fetchall()
    remote_engine.dispose()

def get_remote_status():
    status_dict = {}
    remote_engine = get_wcmkt_remote_engine()
    with remote_engine.connect() as conn:
        tables = get_remote_table_list()
        for table in tables:
            table_name = table[1]
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = count.fetchone()[0]
            status_dict[table_name] = count

    remote_engine.dispose()

    print("Remote Status:")
    print("-" * 20)
    print(status_dict)
    return status_dict

def get_watchlist_ids():
    stmt = text("SELECT DISTINCT type_id FROM watchlist")
    engine = create_engine(wcmkt_local_url)
    with engine.connect() as conn:
        result = conn.execute(stmt)
        watchlist_ids = [row[0] for row in result]
    engine.dispose()
    return watchlist_ids

def get_fit_items(fit_id: int):
    stmt = text("SELECT type_id FROM fittings_fittingitem WHERE fit_id = :fit_id")
    engine = create_engine(fittings_local_url)
    with engine.connect() as conn:
        result = conn.execute(stmt, {"fit_id": fit_id})
        fit_items = [row[0] for row in result]
    engine.dispose()
    return fit_items

def get_fit_ids(doctrine_id: int):
    stmt = text("SELECT fitting_id FROM fittings_doctrine_fittings WHERE doctrine_id = :doctrine_id")
    engine = create_engine(fittings_local_url)
    with engine.connect() as conn:
        result = conn.execute(stmt, {"doctrine_id": doctrine_id})
        fit_ids = [row[0] for row in result]
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
        engine = create_engine(sde_local_url)
        with engine.connect() as conn:
            result = conn.execute(stmt4, {"item": item})
            for row in result:
                type_info = TypeInfo(type_id=item)
                missing_type_info.append(type_info)

    for type_info in missing_type_info:
        stmt5 = text("INSERT INTO watchlist (type_id, type_name, group_name, category_name, category_id, group_id) VALUES (:type_id, :type_name, :group_name, :category_name, :category_id, :group_id)")
        engine = create_engine(wcmkt_local_url)
        with engine.connect() as conn:
            conn.execute(stmt5, {"type_id": type_info.type_id, "type_name": type_info.type_name, "group_name": type_info.group_name, "category_name": type_info.category_name, "category_id": type_info.category_id, "group_id": type_info.group_id})
            conn.commit()
        engine.dispose()
        logger.info(f"Added {type_info.type_name} to watchlist")
        print(f"Added {type_info.type_name} to watchlist")

def add_region_history(history: list[dict]):
    timestamp = datetime.now(timezone.utc)
    timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    engine = create_engine(wcmkt_local_url)
    session = Session(bind=engine)

    with session.begin():
        session.query(RegionHistory).delete()


        for item in history:
            for type_id, history in item.items():
                print(f"Processing type_id: {type_id}, {get_type_name(type_id)}")
                for record in history:
                    date = datetime.strptime(record["date"], "%Y-%m-%d")
                    order = RegionHistory(type_id=type_id, average=record["average"], date=date, highest=record["highest"], lowest=record["lowest"], order_count=record["order_count"], volume=record["volume"], timestamp=datetime.now(timezone.utc))
                    session.add(order)
        session.commit()
        session.close()
        engine.dispose()

def get_region_history()-> pd.DataFrame:
    engine = create_engine(wcmkt_local_url)
    with engine.connect() as conn:
        stmt = text("SELECT * FROM region_history")
        result = conn.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    engine.dispose()
    return df
def get_region_deployment_history(deployment_date: datetime) -> pd.DataFrame:
    """
    Get region history data after a specified deployment date.

    Args:
        deployment_date: datetime object representing the deployment date

    Returns:
        pandas DataFrame containing region history records after the deployment date
    """
    df = get_region_history()

    if df.empty:
        print("No region history data found")
        return df

    # Convert the date column to datetime if it's not already
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])

        # Filter records after the deployment date
        filtered_df = df[df['date'] >= deployment_date].copy()

        # Sort by date for better readability
        filtered_df = filtered_df.sort_values('date')

        print(f"Found {len(filtered_df)} records after {deployment_date.strftime('%Y-%m-%d')}")
        print(f"Date range: {filtered_df['date'].min()} to {filtered_df['date'].max()}")

        return filtered_df
    else:
        print("No 'date' column found in region history data")
        return df

def init_database():
    logger.info("Initializing fittings database")
    conn = fittings_conn()
    conn.sync()
    conn.close()

    logger.info("Initializing market database")
    conn = wcmkt_conn()
    conn.sync()
    conn.close()

    logger.info("Initializing SDE database")
    conn = sde_conn()
    conn.sync()
    conn.close()

if __name__ == "__main__":
    pass