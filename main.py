import time
import json

import pandas as pd
import requests
from requests import ReadTimeout
from logging_config import configure_logging
from ESI_OAUTH_FLOW import get_token
from dbhandler import *
from models import *
from utils import *
from data_processing import *
from utils import get_status
# ---------------------------------------------
# ESI Structure Market Tools for Eve Online
# ---------------------------------------------
# #Developed as a learning project, to access Eve's enfeebled ESI. I'm not a real programmer, ok? Don't laugh at me.
# Contact orthel_toralen on Discord with questions.

# Currently set for the 4-HWWF Keepstar. You can enter another structure ID for a player-owned structure that you have access to.
structure_id = 1035466617946
region_id = 10000003


# set variables for ESI requests
MARKET_STRUCTURE_URL = (
    f"https://esi.evetech.net/latest/markets/structures/{structure_id}/?page="
)
MARKET_HISTORY_URL = (
    f"https://esi.evetech.net/latest/markets/{region_id}/history/?type_id="
)
SCOPE = ["esi-markets.structure_markets.v1"]

# make sure you have this scope enabled in you ESI Dev Application settings.
logger = configure_logging(__name__)


def fetch_market_orders() -> list[dict]:
    token = get_token(SCOPE)
    logger.info("fetching market orders")
    if token:
        logger.info("Token fetched successfully")
    else:
        logger.error("Failed to fetch token")

    headers = {
        "Authorization": f"Bearer {token['access_token']}",
        "Content-Type": "application/json",
        "User-Agent": "WC Markets DEVv0.44 (admin contact: Orthel.Toralen@gmail.com; +https://github.com/OrthelT/ESIMarket_Tool",
    }

    page = 1
    max_pages = 1
    orders = []

    while page <= max_pages:
        try:
            response = requests.get(
                MARKET_STRUCTURE_URL + str(page), headers=headers, timeout=10
            )
            response.raise_for_status()

            if response.status_code == 200:
                max_pages = int(response.headers.get("X-Pages", 1))
            else:
                logger.error(f"Error fetching data: {response.status_code}")
                break
            print(
                f"\rFetching market orders page {page} of {max_pages}",
                end="",
                flush=True,
            )
            data = response.json()
            if isinstance(data, list):
                orders.extend(data)
            page += 1
        except ReadTimeout:
            logger.error(f"Request timed out on page {page}")
            page += 1
        except Exception as e:
            logger.error(f"Error on page {page}: {e}")
            page += 1
            time.sleep(1)
    if orders:
        with open("market_orders.json", "w") as f:
            json.dump(orders, f)
        return orders
    else:
        logger.error("No orders found")
        return None


def fetch_history(watchlist: pd.DataFrame) -> list[dict]:
    logger.info("Fetching history")
    if watchlist is None or watchlist.empty:
        logger.error("No watchlist provided or watchlist is empty")
        return None
    else:
        logger.info("Watchlist found")
        print(f"Watchlist found: {len(watchlist)} items")

    type_ids = watchlist["type_id"].tolist()
    logger.info(f"Fetching history for {len(type_ids)} types")

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "WC Markets DEVv0.44 (admin contact: Orthel.Toralen@gmail.com; +https://github.com/OrthelT/ESIMarket_Tool",
    }

    history = []
    watchlist_length = len(watchlist)
    for i, type_id in enumerate(type_ids):
        item_name = watchlist[watchlist["type_id"] == type_id]["type_name"].values[0]
        try:
            url = f"{MARKET_HISTORY_URL}{type_id}"
            print(
                f"\rFetching history for ({i + 1}/{watchlist_length})",
                end="",
                flush=True,
            )
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            if response.status_code == 200:
                data = response.json()
                for record in data:
                    record["type_name"] = item_name
                    record["type_id"] = type_id

                if isinstance(data, list):
                    history.extend(data)
                else:
                    logger.warning(f"Unexpected data format for {item_name}")
            else:
                logger.error(
                    f"Error fetching history for {item_name}: {response.status_code}"
                )
        except Exception as e:
            logger.error(f"Error processing {item_name}: {e}")
            continue

    if history:
        logger.info(f"Successfully fetched {len(history)} total history records")
        with open("market_history.json", "w") as f:
            json.dump(history, f)
        return history
    else:
        logger.error("No history records found")
        return None


def check_tables():
    tables = ["doctrines", "marketstats", "marketorders", "market_history"]
    engine = sa.create_engine(f"sqlite:///{local_mkt_path}")
    for table in tables:
        print(f"Table: {table}")
        print("=" * 80)
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {table} LIMIT 10"))
            for row in result:
                print(row)
        print("\n")
    engine.dispose()


def main():
    logger.info("Starting mkts-backend")

    # Configure memory management for large datasets
    import gc

    gc.collect()  # Force garbage collection before starting

    # Set pandas options for memory efficiency
    pd.set_option(
        "mode.chained_assignment", None
    )  # Disable warnings for large datasets

    valid_columns = MarketOrders.__table__.columns.keys()

    data = fetch_market_orders()

    if data:
        orders_df = pd.DataFrame.from_records(data)
        orders_df = get_type_names(orders_df)
        orders_df = orders_df[valid_columns]
        orders_df = validate_columns(orders_df, valid_columns)

        print(f"Orders fetched:{len(orders_df)} items")
        logger.info(f"Orders fetched:{len(orders_df)} items")

        update_database("marketorders", orders_df)
        logger.info(f"Orders updated:{get_table_length('marketorders')} items")
    else:
        logger.error("No orders found")

    watchlist = get_watchlist()

    history = fetch_history(watchlist)

    valid_history_columns = MarketHistory.__table__.columns.keys()

    if history:
        history_df = pd.DataFrame.from_records(history)
        history_df = add_timestamp(history_df)
        history_df = add_autoincrement(history_df)
        history_df = validate_columns(history_df, valid_history_columns)

        try:
            update_database("market_history", history_df)
            logger.info(f"History updated:{get_table_length('market_history')} items")
            print(f"History updated:{get_table_length('market_history')} items")
        except Exception as e:
            logger.error(f"Failed to update market history: {e}")
            logger.info("Attempting to clear memory and retry...")
            del history_df
            gc.collect()

            # Recreate the DataFrame and try again with smaller chunks
            history_df = pd.DataFrame.from_records(history)
            history_df = add_timestamp(history_df)
            history_df = add_autoincrement(history_df)
            history_df = validate_columns(history_df, valid_history_columns)

            try:
                update_database("market_history", history_df)
                logger.info(
                    f"History updated on retry:{get_table_length('market_history')} items"
                )
                print(
                    f"History updated on retry:{get_table_length('market_history')} items"
                )
            except Exception as retry_error:
                logger.error(f"Failed to update market history on retry: {retry_error}")
                print("Critical error: Unable to update market history")
    else:
        logger.error("No history found")

    valid_market_stats_columns = MarketStats.__table__.columns.keys()
    market_stats = calculate_market_stats()

    market_stats_df = pd.DataFrame.from_records(market_stats)
    market_stats_df = validate_columns(market_stats_df, valid_market_stats_columns)
    update_database("marketstats", market_stats_df)
    logger.info(f"Market stats updated:{get_table_length('marketstats')} items")

    valid_doctrine_columns = Doctrines.__table__.columns.keys()
    doctrine_stats = calculate_doctrine_stats()
    doctrine_stats["id"] = doctrine_stats.index + 1

    doctrine_stats_df = pd.DataFrame.from_records(doctrine_stats)
    doctrine_stats_df = validate_columns(doctrine_stats_df, valid_doctrine_columns)
    update_database("doctrines", doctrine_stats_df)
    logger.info(f"Doctrines updated:{get_table_length('doctrines')} items")

    valid_doctrine_columns = Doctrines.__table__.columns.keys()
    doctrine_stats = calculate_doctrine_stats()
    doctrine_stats_df = pd.DataFrame.from_records(doctrine_stats)
    doctrine_stats_df = add_autoincrement(doctrine_stats_df)
    doctrine_stats_df = validate_columns(doctrine_stats_df, valid_doctrine_columns)

    update_database("doctrines", doctrine_stats_df)
    logger.info(f"Doctrines updated:{get_table_length('doctrines')} items")
    print(f"Doctrines updated:{get_table_length('doctrines')} items")

    get_status()


if __name__ == "__main__":
    main()
    # Uncomment the line below to check the database tables
    # check_tables()

    # Uncomment the line below to fetch market orders
    # fetch_market_orders()

    # Uncomment the line below to fetch market history
    # fetch_history(pd.read_csv("data/watchlist.csv"))

    # Uncomment the line below to calculate market stats
    # calculate_market_stats()

    # Uncomment the line below to calculate doctrine stats
    # calculate_doctrine_stats()
