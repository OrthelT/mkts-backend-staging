import sys
import pandas as pd
from logging_config import configure_logging
from dbhandler import get_table_length, update_remote_database, process_history, process_market_orders
from models import MarketOrders, MarketStats, Doctrines
from utils import get_type_names, validate_columns, add_timestamp, add_autoincrement, convert_datetime_columns
from data_processing import calculate_market_stats, calculate_doctrine_stats
from sqlalchemy import text
from config import DatabaseConfig, ESIConfig
from esi_requests import fetch_market_orders
import json
from async_history import run_async_history
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

def main(history: bool = False, region_only: bool = False):
    if history:
        logger.info("History mode enabled")
    else:
        logger.info("History mode disabled")

    logger.info("Starting mkts-backend")
    esi = ESIConfig("primary")


    print("=" * 80)
    print("Fetching market orders")
    print("=" * 80)

    data = fetch_market_orders(esi, order_type = "all")
    status = process_market_orders(data)
    with open("data/market_orders_new.json", "w") as f:
        json.dump(data, f)
    if status:
        logger.info(f"Orders updated:{get_table_length('marketorders')} items")
    else:
        logger.error("Failed to update market orders")

    watchlist = DatabaseConfig("wcmkt3").get_watchlist()

    if history:
        logger.info("Processing history")
        data = run_async_history()
        status = process_history(data)
        if status:
            logger.info(f"History updated:{get_table_length('market_history')} items")
        else:
            logger.error("Failed to update market history")
    else:
        logger.info("Skipping history processing")

    logger.info("Calculating market stats")
    valid_market_stats_columns = MarketStats.__table__.columns.keys()
    market_stats_df = calculate_market_stats()

    market_stats_df = validate_columns(market_stats_df, valid_market_stats_columns)

    update_remote_database(MarketStats, market_stats_df)
    logger.info(f"Market stats updated:{get_table_length('marketstats')} items")

    valid_doctrine_columns = Doctrines.__table__.columns.keys()
    doctrine_stats = calculate_doctrine_stats()
    doctrine_stats["id"] = doctrine_stats.index + 1

    doctrine_stats_df = pd.DataFrame.from_records(doctrine_stats)
    # Convert timestamp field to Python datetime objects for SQLite
    doctrine_stats_df = convert_datetime_columns(doctrine_stats_df, ['timestamp'])
    doctrine_stats_df = validate_columns(doctrine_stats_df, valid_doctrine_columns)
    update_remote_database(Doctrines, doctrine_stats_df)
    logger.info(f"Doctrines updated:{get_table_length('doctrines')} items")

    valid_doctrine_columns = Doctrines.__table__.columns.keys()
    doctrine_stats_df = calculate_doctrine_stats()
    doctrine_stats_df = add_autoincrement(doctrine_stats_df)
    # Convert timestamp field to Python datetime objects for SQLite
    doctrine_stats_df = convert_datetime_columns(doctrine_stats_df, ['timestamp'])
    doctrine_stats_df = validate_columns(doctrine_stats_df, valid_doctrine_columns)

    update_remote_database(Doctrines, doctrine_stats_df)
    logger.info(f"Doctrines updated:{get_table_length('doctrines')} items")
    print(f"Doctrines updated:{get_table_length('doctrines')} items")

    print("=" * 80)
    print("Market job complete")
    print("=" * 80)

if __name__ == "__main__":

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

    main(history=include_history)
