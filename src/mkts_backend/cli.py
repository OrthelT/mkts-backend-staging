import sys
import json
import time

from mkts_backend.config.logging_config import configure_logging
from mkts_backend.db.db_queries import get_table_length
from mkts_backend.db.db_handlers import (
    upsert_database,
    update_history,
    update_market_orders,
    log_update,
)
from mkts_backend.db.models import MarketStats, Doctrines
from mkts_backend.utils.utils import (
    validate_columns,
    convert_datetime_columns,
    init_databases,
)
from mkts_backend.processing.data_processing import (
    calculate_market_stats,
    calculate_doctrine_stats,
)
from sqlalchemy import text
from mkts_backend.config.config import DatabaseConfig
from mkts_backend.config.esi_config import ESIConfig
from mkts_backend.esi.esi_requests import fetch_market_orders
from mkts_backend.esi.async_history import run_async_history
from mkts_backend.utils.db_utils import check_updates

logger = configure_logging(__name__)

def check_tables():
    tables = ["doctrines", "marketstats", "marketorders", "market_history"]
    db = DatabaseConfig("wcmkt")
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
    print("Usage: mkts-backend [--history|--include-history] [--check_tables]")
    print("Options:")
    print("  --history | --include-history: Include history processing")
    print("  --check_tables: Check the tables in the database")

def process_market_orders(esi: ESIConfig, order_type: str = "all", test_mode: bool = False) -> bool:
    """Fetches market orders from ESI and updates the database"""
    save_path = "data/market_orders_new.json"
    data = fetch_market_orders(esi, order_type=order_type, test_mode=test_mode)
    if data:
        with open(save_path, "w") as f:
            json.dump(data, f)
        logger.info(f"ESI returned {len(data)} market orders. Saved to {save_path}")
        status = update_market_orders(data)
        if status:
            log_update("marketorders",remote=True)
            logger.info(f"Orders updated:{get_table_length('marketorders')} items")
            return True
        else:
            logger.error(
                "Failed to update market orders. ESI call succeeded but something went wrong updating the database"
            )
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
            log_update("market_history",remote=True)
            logger.info(f"History updated:{get_table_length('market_history')} items")
            return True
        else:
            logger.error("Failed to update market history")
            return False

def process_market_stats():
    logger.info("Calculating market stats")
    logger.info("syncing database")
    db = DatabaseConfig("wcmkt")
    db.sync()
    logger.info("database synced")
    logger.info("validating database")
    validation_test = db.validate_sync()
    if validation_test:
        logger.info("database validated")
    else:
        logger.error("database validation failed")
        raise Exception("database validation failed in market stats")

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
        status = upsert_database(MarketStats, market_stats_df)
        if status:
            log_update("marketstats",remote=True)
            logger.info(f"Market stats updated:{get_table_length('marketstats')} items")
            return True
        else:
            logger.error("Failed to update market stats")
            return False
    except Exception as e:
        logger.error(f"Failed to update market stats: {e}")
        return False

def process_doctrine_stats():
    logger.info("Calculating doctrines stats")
    logger.info("syncing database")
    db = DatabaseConfig("wcmkt")
    db.sync()
    logger.info("database synced")
    logger.info("validating database")
    validation_test = db.validate_sync()
    if validation_test:
        logger.info("database validated")
    else:
        logger.error("database validation failed")
        raise Exception("database validation failed in doctrines stats")

    doctrine_stats_df = calculate_doctrine_stats()
    doctrine_stats_df = convert_datetime_columns(doctrine_stats_df, ["timestamp"])
    status = upsert_database(Doctrines, doctrine_stats_df)
    if status:
        log_update("doctrines",remote=True)
        logger.info(f"Doctrines updated:{get_table_length('doctrines')} items")
        return True
    else:
        logger.error("Failed to update doctrines")
        return False

def main(history: bool = False):
    """Main function to process market orders, history, market stats, and doctrines"""
    # Accept flags when invoked via console_script entrypoint
    if "--check_tables" in sys.argv:
        check_tables()
        return

    if "--history" in sys.argv or "--include-history" in sys.argv:
        history = True

    logger.info(f"sys.argv: {sys.argv}")
    logger.info(f"history: {history}")
    logger.info("=" * 80)
    init_databases()
    logger.info("Databases initialized")

    esi = ESIConfig("primary")
    db = DatabaseConfig("wcmkt")
    logger.info(f"Database: {db.alias}")
    validation_test = db.validate_sync()

    if not validation_test:
        logger.warning("wcmkt database is not up to date. Updating...")
        db.sync()
        logger.info("database synced")
        validation_test = db.validate_sync()
        if validation_test:
            logger.info("database validated")
        else:
            logger.error("database validation failed")
            raise Exception("database validation failed in main")

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

    watchlist = db.get_watchlist()
    if len(watchlist) > 0:
        logger.info(f"Watchlist found: {len(watchlist)} items")
    else:
        logger.error("No watchlist found. Unable to proceed further.")
        exit()

    if history:
        logger.info("Processing history ")
        status = process_history()
        if status:
            logger.info("History updated")
        else:
            logger.error("Failed to update history")
    else:
        logger.info("History mode disabled. Skipping history processing")

    status = process_market_stats()
    if status:
        logger.info("Market stats updated")
    else:
        logger.error("Failed to update market stats")
        exit()

    status = process_doctrine_stats()
    if status:
        logger.info("Doctrines updated")
    else:
        logger.error("Failed to update doctrines")
        exit()

    logger.info("=" * 80)
    logger.info("Market job complete")
    logger.info("=" * 80)

    logger.info("Checking updates")
    logger.info("=" * 80)
    status_dict = check_updates(remote=True)
    logger.info(f"Stats update: {status_dict['stats']['time_since']}")
    logger.info(f"History update: {status_dict['history']['time_since']}")
    logger.info(f"Doctrines update: {status_dict['doctrines']['time_since']}")
    logger.info(f"Orders update: {status_dict['orders']['time_since']}")
    logger.info("=" * 80)
    logger.info("Market job complete")
    logger.info("=" * 80)


if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("Starting mkts-backend")
    logger.info("=" * 80 + "\n")
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
