import pandas as pd
from sqlalchemy import text, insert
from config.config import DatabaseConfig
from config.logging_config import configure_logging
from utils.get_type_info import TypeInfo
from db.models import Watchlist, DeploymentWatchlist
from db.db_queries import get_watchlist_ids, get_fit_ids, get_fit_items

logger = configure_logging(__name__)

sde_db = DatabaseConfig("sde")
wcmkt_db = DatabaseConfig("wcmkt")

def add_missing_items_to_watchlist():
    missing = [21889, 21888, 21890, 47926, 2629, 16274, 17889, 17888, 17887, 41490, 32014, 16273]

    engine = sde_db.engine
    with engine.connect() as conn:
        # Use proper parameter binding for IN clause with SQLite
        from sqlalchemy import bindparam
        stmt = text("SELECT * FROM inv_info WHERE typeID IN :missing").bindparams(bindparam('missing', expanding=True))
        res = conn.execute(stmt, {"missing": missing})
        df = pd.DataFrame(res.fetchall())
        df.columns = res.keys()
        print(df.columns)

    watchlist = wcmkt_db.get_watchlist()
    inv_cols = ['typeID', 'typeName', 'groupID', 'groupName', 'categoryID',
       'categoryName']
    df = df[inv_cols]
    watchlist_cols = ['type_id', 'type_name', 'group_id', 'group_name', 'category_id', 'category_name']
    df = df.rename(columns=dict(zip(inv_cols, watchlist_cols)))
    df.to_csv("data/watchlist_missing.csv", index=False)
    watchlist = pd.concat([watchlist, df], ignore_index=True)
    watchlist.to_csv("data/watchlist_updated.csv", index=False)

def update_watchlist_tables(missing_items: list[int]):
    """Update both watchlist and deployment_watchlist tables with missing items"""

    # Get missing items from SDE
    engine = sde_db.engine
    with engine.connect() as conn:
        from sqlalchemy import bindparam
        stmt = text("SELECT * FROM inv_info WHERE typeID IN :missing").bindparams(bindparam('missing', expanding=True))
        res = conn.execute(stmt, {"missing": missing_items})
        df = pd.DataFrame(res.fetchall())
        df.columns = res.keys()

    # Prepare data for database insertion
    inv_cols = ['typeID', 'typeName', 'groupID', 'groupName', 'categoryID', 'categoryName']
    watchlist_cols = ['type_id', 'type_name', 'group_id', 'group_name', 'category_id', 'category_name']
    df = df[inv_cols]
    df = df.rename(columns=dict(zip(inv_cols, watchlist_cols)))

    # Update watchlist table
    engine = wcmkt_db.engine
    with engine.connect() as conn:
        # Insert into watchlist table
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

        # Insert into deployment_watchlist table
        for _, row in df.iterrows():
            stmt = insert(DeploymentWatchlist).values(
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
                logger.info(f"Added {row['type_name']} (ID: {row['type_id']}) to deployment_watchlist")
            except Exception as e:
                logger.warning(f"Item {row['type_id']} may already exist in deployment_watchlist: {e}")

    logger.info(f"Updated both watchlist and deployment_watchlist tables with {len(df)} missing items")

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
    logger.info(f"Adding {len(missing_fit_items)} missing items to watchlist")
    print(f"Adding {len(missing_fit_items)} missing items to watchlist")
    continue_adding = input("Continue adding? (y/n)")
    if continue_adding == "n":
        return
    else:
        logger.info(f"Continuing to add {len(missing_fit_items)} missing items to watchlist")
        print(f"Continuing to add {len(missing_fit_items)} missing items to watchlist")

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
        db = DatabaseConfig("wcmkt")
        engine = db.engine
        with engine.connect() as conn:
            conn.execute(stmt5, {"type_id": type_info.type_id, "type_name": type_info.type_name, "group_name": type_info.group_name, "category_name": type_info.category_name, "category_id": type_info.category_id, "group_id": type_info.group_id})
            conn.commit()
        conn.close()
        engine.dispose()
        logger.info(f"Added {type_info.type_name} to watchlist")
        print(f"Added {type_info.type_name} to watchlist")

#not currently used
# def get_system_market_value(system_id: int) -> float:
#     """
#     Convenience function to get total market value for a system

#     Args:
#         system_id: System ID to calculate market value for

#     Returns:
#         Total market value as float
#     """
#     market_data = process_system_orders(system_id)
#     return calculate_total_market_value(market_data)

# def calculate_total_ship_count(market_data: pd.DataFrame) -> int:
#     """
#     Calculate the total number of ships on the market

#     Args:
#         market_data: DataFrame from process_system_orders containing category_name and volume_remain columns

#     Returns:
#         Total ship count as int
#     """
#     if market_data is None or market_data.empty:
#         logger.warning("No market data provided for ship count calculation")
#         print("No market data provided for ship count calculation")
#         return 0

#     # Filter for ships only and sum volume_remain
#     ships_data = market_data[market_data['category_name'] == 'Ship']
#     total_ship_count = ships_data['volume_remain'].sum()

#     logger.info(f"Total ships on market: {total_ship_count:,}")
#     print(f"Total ships on market: {total_ship_count:,}")
#     return int(total_ship_count)

# def get_system_ship_count(system_id: int) -> int:
    # """
    # Convenience function to get total ship count for a system

    # Args:
    #     system_id: System ID to calculate ship count for

    # Returns:
    #     Total ship count as int
    # """
    # market_data = process_system_orders(system_id)
    # return calculate_total_ship_count(market_data)

if __name__ == "__main__":
    pass