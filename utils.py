import pandas as pd
from logging_config import configure_logging
import time
import json
import sqlalchemy as sa
from sqlalchemy import text
from models import wcmkt_path

logger = configure_logging(__name__)


def get_null_count(df):
    return df.isnull().sum()


def validate_columns(df, valid_columns):
    return df[valid_columns]


def validate_type_names(df):
    return df[df["type_name"].notna()]


def validate_type_ids(df):
    return df[df["type_id"].notna()]


def validate_order_ids(df):
    return df[df["order_id"].notna()]


def add_timestamp(df):
    df["timestamp"] = pd.Timestamp.now()
    return df


def add_autoincrement(df):
    df["id"] = df.index + 1
    return df


def standby(seconds: int):
    for i in range(seconds):
        message = f"\rWaiting for {seconds - i} seconds"
        print(message, end="", flush=True)
        time.sleep(1)
    print()


def simulate_market_orders() -> dict:
    with open("market_orders.json", "r") as f:
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


def get_status() -> dict:
    engine = sa.create_engine(f"sqlite:///{wcmkt_path}")
    with engine.connect() as conn:
        dcount = conn.execute(text("SELECT COUNT(id) FROM doctrines"))
        doctrine_count = dcount.fetchone()[0]
        order_count = conn.execute(text("SELECT COUNT(order_id) FROM marketorders"))
        order_count = order_count.fetchone()[0]
        history_count = conn.execute(text("SELECT COUNT(id) FROM market_history"))
        history_count = history_count.fetchone()[0]
        stats_count = conn.execute(text("SELECT COUNT(type_id) FROM marketstats"))
        stats_count = stats_count.fetchone()[0]
    engine.dispose()
    print(f"Doctrines: {doctrine_count}")
    print(f"Market Orders: {order_count}")
    print(f"Market History: {history_count}")
    print(f"Market Stats: {stats_count}")

    status_dict = {
        "doctrines": doctrine_count,
        "market_orders": order_count,
        "market_history": history_count,
        "market_stats": stats_count,
    }

    timestamp = time.time()
    with open(f"status_{timestamp}.json", "w") as f:
        json.dump(status_dict, f)


if __name__ == "__main__":
    pass
