import pandas as pd
from sqlalchemy import text, insert, create_engine, select
from mkts_backend.config.config import DatabaseConfig
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.db.models import Watchlist, UpdateLog
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session

logger = configure_logging(__name__)

sde_db = DatabaseConfig("sde")
wcmkt_db = DatabaseConfig("wcmkt")

def add_missing_items_to_watchlist(missing_items: list[int]):
    engine = sde_db.engine
    with engine.connect() as conn:
        from sqlalchemy import bindparam
        stmt = text("SELECT * FROM inv_info WHERE typeID IN :missing").bindparams(bindparam('missing', expanding=True))
        res = conn.execute(stmt, {"missing": missing_items})
        df = pd.DataFrame(res.fetchall())
        df.columns = res.keys()
        df = df.rename(columns={"typeID": "type_id", "typeName": "type_name", "groupID": "group_id", "groupName": "group_name", "categoryID": "category_id", "categoryName": "category_name"})

    watchlist = wcmkt_db.get_watchlist()
    inv_cols = ['type_id', 'type_name', 'group_id', 'group_name', 'category_id', 'category_name']
    df = df[inv_cols]
    watchlist = pd.concat([watchlist, df], ignore_index=True)
    watchlist.to_csv("data/watchlist_updated.csv", index=False)
    watchlist.to_sql("watchlist", wcmkt_db.engine, if_exists="append", index=False)

def update_watchlist_tables(missing_items: list[int]):
    engine = sde_db.engine
    with engine.connect() as conn:
        from sqlalchemy import bindparam
        stmt = text("SELECT * FROM inv_info WHERE typeID IN :missing").bindparams(bindparam('missing', expanding=True))
        res = conn.execute(stmt, {"missing": missing_items})
        df = pd.DataFrame(res.fetchall())
        df.columns = res.keys()

    inv_cols = ['typeID', 'typeName', 'groupID', 'groupName', 'categoryID', 'categoryName']
    watchlist_cols = ['type_id', 'type_name', 'group_id', 'group_name', 'category_id', 'category_name']
    df = df[inv_cols]
    df = df.rename(columns=dict(zip(inv_cols, watchlist_cols)))

    engine = wcmkt_db.engine
    with engine.connect() as conn:
        for _, row in df.iterrows():
            stmt = insert(Watchlist).values(
                type_id=row['type_id'],
                type_name=row['type_name'],
                group_id=row['group_id'],
                group_name=row['group_name'],
                category_id=row['category_id'],
                category_name=row['category_name']
            )
            try:
                conn.execute(stmt)
                conn.commit()
                logger.info(f"Added {row['type_name']} (ID: {row['type_id']}) to watchlist")
            except Exception as e:
                logger.warning(f"Item {row['type_id']} may already exist in watchlist: {e}")


def restore_doctrines_from_backup(backup_db_path: str, target_db_alias: str = "wcmkt"):
    """
    Restore doctrines table from a backup database file.

    Args:
        backup_db_path: Path to the backup database file (e.g., "backup_wcmkt2.db")
        target_db_alias: Target database alias to restore to (default: "wcmkt")
    """
    logger.info(f"Starting doctrines restoration from backup: {backup_db_path}")

    try:
        # Connect to backup database
        backup_engine = create_engine(f"sqlite:///{backup_db_path}")

        # Check if doctrines table exists in backup
        with backup_engine.connect() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='doctrines'"))
            if not result.fetchone():
                logger.error(f"Doctrines table not found in backup database: {backup_db_path}")
                return False

            # Get all doctrines data from backup
            doctrines_df = pd.read_sql_query("SELECT * FROM doctrines", conn)
            logger.info(f"Found {len(doctrines_df)} doctrines records in backup")

        # Connect to target database
        target_db = DatabaseConfig(target_db_alias)
        target_engine = target_db.remote_engine

        # Clear existing doctrines table
        with target_engine.connect() as conn:
            conn.execute(text("DELETE FROM doctrines"))
            conn.commit()
            logger.info("Cleared existing doctrines table")

            # Insert backup data
            doctrines_df.to_sql("doctrines", conn, if_exists="append", index=False)
            conn.commit()
            logger.info(f"Restored {len(doctrines_df)} doctrines records to target database")

        # Verify restoration
        with target_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM doctrines"))
            count = result.scalar()
            logger.info(f"Verification: {count} doctrines records in target database")

        return True

    except Exception as e:
        logger.error(f"Error restoring doctrines from backup: {e}")
        return False
    finally:
        if 'backup_engine' in locals():
            backup_engine.dispose()
        if 'target_engine' in locals():
            target_engine.dispose()

def merge_doctrines_with_backup(backup_db_path: str, target_db_alias: str = "wcmkt"):
    """
    Merge doctrines from backup with existing data, avoiding duplicates.

    Args:
        backup_db_path: Path to the backup database file
        target_db_alias: Target database alias to merge into
    """
    logger.info(f"Starting doctrines merge from backup: {backup_db_path}")

    try:
        # Connect to backup database
        backup_engine = create_engine(f"sqlite:///{backup_db_path}")

        # Get doctrines from backup
        with backup_engine.connect() as conn:
            backup_df = pd.read_sql_query("SELECT * FROM doctrines", conn)
            logger.info(f"Found {len(backup_df)} doctrines records in backup")

        # Connect to target database
        target_db = DatabaseConfig(target_db_alias)
        target_engine = target_db.engine

        # Get existing doctrines from target
        with target_engine.connect() as conn:
            existing_df = pd.read_sql_query("SELECT * FROM doctrines", conn)
            logger.info(f"Found {len(existing_df)} existing doctrines records")

        # Merge dataframes, keeping existing data and adding new from backup
        if not existing_df.empty:
            # Use fit_id as unique identifier for merging
            merged_df = pd.concat([existing_df, backup_df], ignore_index=True)
            # Remove duplicates based on fit_id, ship_id, type_id combination
            merged_df = merged_df.drop_duplicates(subset=['fit_id', 'ship_id', 'type_id'], keep='first')
            logger.info(f"Merged to {len(merged_df)} unique doctrines records")
        else:
            merged_df = backup_df
            logger.info("No existing data, using backup data as-is")

        # Clear and restore merged data
        with target_engine.connect() as conn:
            conn.execute(text("DELETE FROM doctrines"))
            conn.commit()
            merged_df.to_sql("doctrines", conn, if_exists="append", index=False)
            conn.commit()
            logger.info(f"Restored {len(merged_df)} merged doctrines records")

        return True

    except Exception as e:
        logger.error(f"Error merging doctrines with backup: {e}")
        return False
    finally:
        if 'backup_engine' in locals():
            backup_engine.dispose()
        if 'target_engine' in locals():
            target_engine.dispose()

def export_doctrines_to_csv(db_alias: str = "wcmkt", output_file: str = "doctrines_backup.csv"):
    """
    Export doctrines table to CSV for backup purposes.

    Args:
        db_alias: Database alias to export from
        output_file: Output CSV file path
    """
    logger.info(f"Exporting doctrines from {db_alias} to {output_file}")

    try:
        db = DatabaseConfig(db_alias)
        engine = db.remote_engine

        with engine.connect() as conn:
            doctrines_df = pd.read_sql_query("SELECT * FROM doctrines", conn)
            doctrines_df.to_csv(output_file, index=False)
            logger.info(f"Exported {len(doctrines_df)} doctrines records to {output_file}")

        return True

    except Exception as e:
        logger.error(f"Error exporting doctrines: {e}")
        return False
    finally:
        if 'engine' in locals():
            engine.dispose()

def get_most_recent_updates(table_name: str, remote: bool = False):

    db = DatabaseConfig("wcmkt")
    engine = db.remote_engine if remote else db.engine
    session = Session(bind=engine)
    with session.begin():
        updates = select(UpdateLog.timestamp).where(UpdateLog.table_name == table_name).order_by(UpdateLog.timestamp.desc())
        result = session.execute(updates).scalar_one()
    session.close()
    engine.dispose()
    return result

def check_updates(remote: bool = False):
    update_status = {
        "stats": {
            "updated": None,
            "needs_update": False,
            "time_since": None
        },
        "history": {
            "updated": None,
            "needs_update": False,
            "time_since": None
        },
        "doctrines": {
            "updated": None,
            "needs_update": False,
            "time_since": None
        },
        "orders": {
            "updated": None,
            "needs_update": False,
            "time_since": None
        }
    }
    logger.info("Checking updates")
    try:
        statsupdate = get_most_recent_updates("marketstats",remote=remote).replace(tzinfo=timezone.utc)
        update_status["stats"]["updated"] = statsupdate
    except Exception as e:
        logger.error(f"Error getting stats update: {e}")

    try:
        historyupdate = get_most_recent_updates("market_history",remote=remote).replace(tzinfo=timezone.utc)
        update_status["history"]["updated"] = historyupdate
    except Exception as e:
        logger.error(f"Error getting history update: {e}")

    try:
        doctrinesupdate = get_most_recent_updates("doctrines",remote=remote).replace(tzinfo=timezone.utc)
        update_status["doctrines"]["updated"] = doctrinesupdate
    except Exception as e:
        logger.error(f"Error getting doctrines update: {e}")

    try:
        ordersupdate = get_most_recent_updates("marketorders",remote=remote).replace(tzinfo=timezone.utc)
        update_status["orders"]["updated"] = ordersupdate
    except Exception as e:
        logger.error(f"Error getting orders update: {e}")

    now = datetime.now(timezone.utc)

    time_since_stats_update = now - update_status["stats"]["updated"]
    time_since_history_update = now - update_status["history"]["updated"]
    time_since_doctrines_update = now - update_status["doctrines"]["updated"]
    time_since_orders_update = now - update_status["orders"]["updated"]

    update_status["stats"]["time_since"] = time_since_stats_update
    update_status["history"]["time_since"] = time_since_history_update
    update_status["doctrines"]["time_since"] = time_since_doctrines_update
    update_status["orders"]["time_since"] = time_since_orders_update

    logger.info(f"Time since stats update: {time_since_stats_update}")
    logger.info(f"Time since history update: {time_since_history_update}")
    logger.info(f"Time since doctrines update: {time_since_doctrines_update}")
    logger.info(f"Time since orders update: {time_since_orders_update}")

    update_status["stats"]["needs_update"] = False
    update_status["history"]["needs_update"] = False
    update_status["doctrines"]["needs_update"] = False
    update_status["orders"]["needs_update"] = False

    if update_status["stats"]["time_since"] > timedelta(hours=1):
        logger.info("Stats update is older than 1 hour")
        logger.info(f"Stats update timestamp: {update_status['stats']['updated']}")
        logger.info(f"Now: {now}")
        update_status["stats"]["needs_update"] = True
    if update_status["history"]["time_since"] > timedelta(hours=1):
        logger.info("History update is older than 1 hour")
        logger.info(f"History update timestamp: {update_status['history']['updated']}")
        logger.info(f"Now: {now}")
        update_status["history"]["needs_update"] = True
    if update_status["doctrines"]["time_since"] > timedelta(hours=1):
        logger.info("Doctrines update is older than 1 hour")
        logger.info(f"Doctrines update timestamp: {update_status['doctrines']['updated']}")
        logger.info(f"Now: {now}")
        update_status["doctrines"]["needs_update"] = True
    if update_status["orders"]["time_since"] > timedelta(hours=1):
        logger.info("Orders update is older than 1 hour")
        logger.info(f"Orders update timestamp: {update_status['orders']['updated']}")
        logger.info(f"Now: {now}")
        update_status["orders"]["needs_update"] = True

    return update_status

def get_time_since_update(table_name: str, remote: bool = False):
    status = check_updates(remote=remote)
    return status.get(table_name).get("time_since")
if __name__ == "__main__":
    pass