import pandas as pd
import os
from logging_config import configure_logging
import time
import json
import sqlalchemy as sa
from sqlalchemy import text, create_engine
from proj_config import sde_local_url, wcmkt_local_url, wc_fittings_local_db_url, wcmkt_db_path
import requests
import libsql

logger = configure_logging(__name__)

def get_type_names(df: pd.DataFrame) -> pd.DataFrame:
    engine = sa.create_engine(sde_local_url)
    with engine.connect() as conn:
        stmt = text("SELECT typeID, typeName, groupName, categoryName FROM inv_info")
        res = conn.execute(stmt)
        df = pd.DataFrame(res.fetchall(), columns=["typeID", "typeName", "groupName", "categoryName"])
        df = df.rename(columns={"typeID": "type_id", "typeName": "type_name", "groupName": "group_name", "categoryName": "category_name"})
    engine.dispose()
    return df[["type_id", "type_name", "group_name", "category_name"]]

def get_type_name(type_id: int) -> str:
    engine = sa.create_engine(sde_local_url)
    with engine.connect() as conn:
        stmt = text("SELECT typeName FROM inv_info WHERE typeID = :type_id")
        res = conn.execute(stmt, {"type_id": type_id})
        type_name = res.fetchone()[0]
    engine.dispose()
    return type_name

def get_type_names_from_esi(df: pd.DataFrame) -> pd.DataFrame:
    type_ids = df["type_id"].unique().tolist()
    logger.info(f"Total unique type IDs: {len(type_ids)}")

    # Process type IDs in chunks of 1000 (ESI limit)
    chunk_size = 1000
    all_names = []

    for i in range(0, len(type_ids), chunk_size):
        chunk = type_ids[i : i + chunk_size]
        logger.info(f"Processing chunk {i // chunk_size + 1}, size: {len(chunk)}")

        url = "https://esi.evetech.net/latest/universe/names/?datasource=tranquility"
        headers = {"User-Agent": "mkts-backend", "Accept": "application/json"}
        response = requests.post(url, headers=headers, json=chunk)

        if response.status_code == 200:
            chunk_names = response.json()
            if chunk_names:
                all_names.extend(chunk_names)
            else:
                logger.warning(f"No names found for chunk {i // chunk_size + 1}")
        else:
            logger.error(
                f"Error fetching names for chunk {i // chunk_size + 1}: {response.status_code}"
            )
            logger.error(f"Response: {response.json()}")

    if all_names:
        names_df = pd.DataFrame.from_records(all_names)
        names_df = names_df.drop(columns=["category"])
        names_df = names_df.rename(columns={"name": "type_name", "id": "type_id"})
        df = df.merge(names_df, on="type_id", how="left")
        return df
    else:
        logger.error("No names found for any chunks")
        return None

def get_null_count(df):
    return df.isnull().sum()

def validate_columns(df, valid_columns):
    return df[valid_columns]

def add_timestamp(df):
    df["timestamp"] = pd.Timestamp.now(tz="UTC")
    # Convert to Python datetime objects that SQLite can handle
    df["timestamp"] = df["timestamp"].dt.tz_convert(None)
    return df

def add_autoincrement(df):
    df["id"] = df.index + 1
    return df

def convert_datetime_columns(df, datetime_columns):
    """Convert string datetime columns to Python datetime objects for SQLite compatibility"""
    for col in datetime_columns:
        if col in df.columns:
            # Convert string datetime to pandas datetime, then to Python datetime
            df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert(None)
    return df

def standby(seconds: int):
    for i in range(seconds):
        message = f"\rWaiting for {seconds - i} seconds"
        print(message, end="", flush=True)
        time.sleep(1)
    print()


def simulate_market_orders() -> dict:
    with open("data/market_orders.json", "r") as f:
        data = json.load(f)
    return data


def simulate_market_history() -> dict:
    df = pd.read_csv("data/valemarkethistory_2025-05-13_08-06-00.csv")
    watchlist = pd.read_csv("data/all_watchlist.csv")
    watchlist = watchlist[["type_id", "type_name"]]
    df = df.merge(watchlist, on="type_id", how="left")
    df = df[
        [
            "average",
            "date",
            "highest",
            "lowest",
            "order_count",
            "volume",
            "type_name",
            "type_id",
        ]
    ]
    return df.to_dict(orient="records")

def get_status():
    engine = sa.create_engine(wcmkt_local_url)
    with engine.connect() as conn:
        dcount = conn.execute(text("SELECT COUNT(id) FROM doctrines"))
        doctrine_count = dcount.fetchone()[0]
        order_count = conn.execute(text("SELECT COUNT(order_id) FROM marketorders"))
        order_count = order_count.fetchone()[0]
        history_count = conn.execute(text("SELECT COUNT(id) FROM market_history"))
        history_count = history_count.fetchone()[0]
        stats_count = conn.execute(text("SELECT COUNT(type_id) FROM marketstats"))
        stats_count = stats_count.fetchone()[0]
        region_orders_count = conn.execute(text("SELECT COUNT(order_id) FROM region_orders"))
        region_orders_count = region_orders_count.fetchone()[0]
    engine.dispose()
    print(f"Doctrines: {doctrine_count}")
    print(f"Market Orders: {order_count}")
    print(f"Market History: {history_count}")
    print(f"Market Stats: {stats_count}")
    print(f"Region Orders: {region_orders_count}")

def get_fit_items(fit_id: int) -> pd.DataFrame:
    table_list_stmt = "SELECT type_id, quantity FROM fittings_fittingitem WHERE fit_id = (:fit_id)"
    engine = create_engine(wc_fittings_local_db_url)
    raptor_fit = []
    with engine.connect() as conn:
        result = conn.execute(text(table_list_stmt), {"fit_id": fit_id})
        table_info = result.fetchall()
        for row in table_info:
            type_id = row.type_id
            fit_qty = row.quantity
            raptor_fit.append({"type_id": type_id, "fit_qty": fit_qty})
        conn.close
    engine.dispose

    for row in raptor_fit:
        type_id = row["type_id"]
        type_name = get_type_name(type_id)
        row["type_name"] = type_name

    df = pd.DataFrame(raptor_fit)
    return df


if __name__ == "__main__":
    pass
