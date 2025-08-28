import sys
import pandas as pd
from logging_config import configure_logging
from dbhandler import get_table_length, upsert_remote_database, update_history, update_market_orders
from models import MarketOrders, MarketStats, Doctrines
from utils import get_type_names, validate_columns, add_timestamp, add_autoincrement, convert_datetime_columns
from data_processing import calculate_market_stats, calculate_doctrine_stats
from sqlalchemy import text
from config import DatabaseConfig, ESIConfig
from esi_requests import fetch_market_orders
import json
from async_history import run_async_history
import time
# ---------------------------------------------
# ESI Structure Market Tools for Eve Online
# ---------------------------------------------
# #Developed as a learning project, to access Eve's enfeebled ESI. I'm not a real programmer, ok? Don't laugh at me.
# Contact orthel_toralen on Discord with questions.

logger = configure_logging(__name__)

def check_tables():
    tables = ["doctrines", "marketstats", "marketorders", "market_history"]
    db = DatabaseConfig("wcmkt3")
    tables = db.get_table_list()

    for table in tables:
        print(f"Table: {table}")
        print("=" * 80)
        with db.engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {table} LIMIT 10"))
            for row in result:
                print(row)
            print("\n")
        conn.close()
    db.engine.dispose()

def display_cli_help():
    print("Usage: python main.py [--history] [--region_only] [--check_tables]")
    print("Options:")
    print("  --history: Include history processing")
    print("  --region_only: Only process region orders")
    print("  --check_tables: Check the tables in the database")

def process_market_orders(esi: ESIConfig, order_type: str = "all", test_mode: bool = False) -> bool:
    """Fetches market orders from ESI and updates the database
    args:
        esi: ESIConfig object
        order_type: str, "all", "buy", "sell"
        test_mode: bool, if True, only fetches 5 pages of orders
    returns:
        bool, True if all orders completed successfully, False otherwise
    """
    save_path = "data/market_orders_new.json" #define a path to save a local backup of most recent market orders
    data = fetch_market_orders(esi, order_type = order_type, test_mode=test_mode)
    if data:
        with open(save_path, "w") as f:
            json.dump(data, f)
        logger.info(f"ESI returned {len(data)} market orders. Saved to {save_path}")
        status = update_market_orders(data) #returns a bool confirming that all downstream db operations completed
        if status:
            logger.info(f"Orders updated:{get_table_length('marketorders')} items")
            return True #returns a Bool to confirm that all orders completed successfully
        else:
            logger.error("Failed to update market orders. ESI call succeeded but something went wrong updating the database")
            return False
    else:
        logger.error("no data returned from ESI call.")
        return False

def process_history():
    logger.info("History mode enabled")
    logger.info("Processing history")
    data = run_async_history()
    if data:
        with open("data/market_history_new.json", "w") as f:
            json.dump(data, f)
        status = update_history(data)
        if status:
            logger.info(f"History updated:{get_table_length('market_history')} items")
            return True
        else:
            logger.error("Failed to update market history")
            return False

def process_market_stats():
    logger.info("Calculating market stats")
    try:
        market_stats_df = calculate_market_stats()
        if len(market_stats_df) > 0:
            logger.info(f"Market stats calculated: {len(market_stats_df)} items")
        else:
            logger.error("Failed to calculate market stats")
            return False
    except Exception as e:
        logger.error(f"Failed to calculate market stats: {e}")
        return False
    try:
        logger.info("Validating market stats columns")
        valid_market_stats_columns = MarketStats.__table__.columns.keys()
        market_stats_df = validate_columns(market_stats_df, valid_market_stats_columns)
        if len(market_stats_df) > 0:
            logger.info(f"Market stats validated: {len(market_stats_df)} items")
        else:
            logger.error("Failed to validate market stats")
            return False
    except Exception as e:
        logger.error(f"Failed to get market stats columns: {e}")
        return False
    try:
        logger.info("Updating market stats in database")
        status = upsert_remote_database(MarketStats, market_stats_df)
        if status:
            logger.info(f"Market stats updated:{get_table_length('marketstats')} items")
            return True
        else:
            logger.error("Failed to update market stats")
            return False
    except Exception as e:
        logger.error(f"Failed to update market stats: {e}")
        return False

def process_doctrine_stats():

    doctrine_stats_df = calculate_doctrine_stats()
    # Convert timestamp field to Python datetime objects for SQLite
    doctrine_stats_df = convert_datetime_columns(doctrine_stats_df, ['timestamp'])
    status = upsert_remote_database(Doctrines, doctrine_stats_df)
    if status:
        logger.info(f"Doctrines updated:{get_table_length('doctrines')} items")
        return True
    else:
        logger.error("Failed to update doctrines")
        return False

def main(history: bool = False):
    """Main function to process market orders, history, market stats, and doctrines"""
    #initialize the logger
    logger.info("Starting main function")
    esi = ESIConfig("primary")
    db = DatabaseConfig("wcmkt3")

    #process market orders
    print("=" * 80)
    print("Fetching market orders")
    print("=" * 80)
    status = process_market_orders(esi, order_type="all", test_mode=False)
    if status:
        logger.info("Market orders updated")
    else:
        logger.error("Failed to update market orders")
        exit()

    logger.info("=" * 80)

    #process history
    watchlist = db.get_watchlist()
    if len(watchlist) > 0:
        logger.info(f"Watchlist found: {len(watchlist)} items")
    else:
        logger.error("No watchlist found. Unable to proceed further.")
        exit()
    logger.info("=" * 80)

    if history:
        logger.info("Processing history ")
        status = process_history()
        if status:
            logger.info("History updated")
        else:
            logger.error("Failed to update history")
    else:
        logger.info("History mode disabled. Skipping history processing")

    logger.info("=" * 80)

    #process market stats
    status = process_market_stats()
    if status:
        logger.info("Market stats updated")
    else:
        logger.error("Failed to update market stats")
        exit()

    logger.info("=" * 80)

    #process doctrines
    status = process_doctrine_stats()
    if status:
        logger.info("Doctrines updated")
    else:
        logger.error("Failed to update doctrines")
        exit()

    logger.info("=" * 80)
    logger.info("Market job complete")
    logger.info("=" * 80)

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("Starting mkts-backend")
    logger.info("=" * 80 + "\n")
    # Check for command line arguments
    include_history = False
    if len(sys.argv) > 1:
        if "--history" in sys.argv:
            include_history = True
        elif "--check_tables" in sys.argv:
            check_tables()
            exit()
        else:
            display_cli_help()
            exit()

    t0 = time.perf_counter()
    main(history=include_history)
    logger.info(f"Main function completed in {time.perf_counter()-t0:.1f}s")
