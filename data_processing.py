import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text, insert, select
from sqlalchemy.orm import Session, query
from logging_config import configure_logging
from models import MarketStats, Doctrines, Watchlist, NakahWatchlist, RegionHistory, RegionOrders, RegionStats, DeploymentWatchlist
import libsql
import json
from proj_config import wcmkt_db_path, wcmkt_local_url, sde_local_path, sde_local_url

from dbhandler import get_watchlist

watchlist = get_watchlist()

logger = configure_logging(__name__)

def calculate_5_percentile_price() -> pd.DataFrame:
    query = """
    SELECT
    type_id,
    price
    FROM marketorders
    WHERE is_buy_order = 0
    """

    engine = sa.create_engine(wcmkt_local_url)

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    engine.dispose()
    df = df.groupby("type_id")["price"].quantile(0.05).reset_index()
    df.price = df.price.apply(lambda x: round(x, 2))
    df.columns = ["type_id", "5_perc_price"]
    return df


def calculate_market_stats() -> pd.DataFrame:
    query = """
    SELECT
    w.type_id,
    w.type_name,
    w.group_name,
    w.category_name,
    w.category_id,
    w.group_id,
    o.min_price,
    o.total_volume_remain,
    h.avg_price,
    h.avg_volume,
    ROUND(CASE
    WHEN h.avg_volume > 0 THEN o.total_volume_remain / h.avg_volume
    ELSE 0
    END, 2) as days_remaining

    FROM watchlist w

    LEFT JOIN (
    SELECT
        type_id, 
        MIN(price) as min_price,
        SUM(volume_remain) as total_volume_remain
    FROM marketorders
        WHERE is_buy_order = 0
        GROUP BY type_id
    ) AS o
    ON w.type_id = o.type_id
    LEFT JOIN (
    SELECT
        type_id,
        AVG(average) as avg_price,
        AVG(volume) as avg_volume
    FROM market_history
    WHERE date >= DATE('now', '-30 day') AND average > 0 AND volume > 0
    GROUP BY type_id
    ) AS h ON w.type_id = h.type_id
    """

    engine = sa.create_engine(wcmkt_local_url)

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
        logger.info(f"df: {df}")
        logger.info(f"Market stats queried: {df.shape[0]} items")

    engine.dispose()

    logger.info(f"Calculating 5 percentile price")
    df2 = calculate_5_percentile_price()
    logger.info(f"Merging 5 percentile price with market stats")
    df = df.merge(df2, on="type_id", how="left")

    logger.info(f"Renaming columns")
    df.days_remaining = df.days_remaining.apply(lambda x: round(x, 1))
    df = df.rename(columns={"5_perc_price": "price"})
    df["last_update"] = pd.Timestamp.now(tz="UTC")
 
    db_cols = MarketStats.__table__.columns.keys()
    df = df[db_cols]
    
    df = df.infer_objects()
    df = df.fillna(0)
    
    df["avg_price"] = df["avg_price"].apply(lambda x: round(x, 2) if x > 0 else 0)
    df["avg_volume"] = df["avg_volume"].apply(lambda x: round(x, 1)if x > 0 else 0)

    df["total_volume_remain"] = df["total_volume_remain"].astype(int)
    logger.info(f"Market stats calculated: {df.shape[0]} items")
    return df

def calculate_doctrine_stats() -> pd.DataFrame:
    doctrine_query = """
    SELECT
    *
    FROM doctrines
    """
    stats_query = """
    SELECT
    *
    FROM marketstats
    """
    engine = sa.create_engine(wcmkt_local_url)

    with engine.connect() as conn:
        doctrine_stats = pd.read_sql_query(doctrine_query, conn)
        market_stats = pd.read_sql_query(stats_query, conn)
    doctrine_stats = doctrine_stats.drop(
        columns=["hulls", "fits_on_mkt", "total_stock", "avg_vol", "days", "timestamp"]
    )
    doctrine_stats["hulls"] = doctrine_stats["ship_id"].map(
        market_stats.set_index("type_id")["total_volume_remain"]
    )
    doctrine_stats["total_stock"] = doctrine_stats["type_id"].map(
        market_stats.set_index("type_id")["total_volume_remain"]
    )
    doctrine_stats["price"] = doctrine_stats["type_id"].map(
        market_stats.set_index("type_id")["price"]
    )
    doctrine_stats["avg_vol"] = doctrine_stats["type_id"].map(
        market_stats.set_index("type_id")["avg_volume"]
    )
    doctrine_stats["days"] = doctrine_stats["type_id"].map(
        market_stats.set_index("type_id")["days_remaining"]
    )
    doctrine_stats["timestamp"] = doctrine_stats["type_id"].map(
        market_stats.set_index("type_id")["last_update"]
    )
    doctrine_stats["fits_on_mkt"] = round(
        doctrine_stats["total_stock"] / doctrine_stats["fit_qty"], 1
    )
    doctrine_stats = doctrine_stats.infer_objects()
    doctrine_stats = doctrine_stats.fillna(0)
    doctrine_stats["fits_on_mkt"] = doctrine_stats["fits_on_mkt"].astype(int)
    doctrine_stats["avg_vol"] = doctrine_stats["avg_vol"].astype(int)
    doctrine_stats = doctrine_stats.reset_index(drop=True)
    
    val_cols = Doctrines.__table__.columns.keys()
    col_compare = set(doctrine_stats.columns) - set(val_cols)

    return doctrine_stats


def calculate_region_5_percentile_price() -> pd.DataFrame:
    """Calculate 5th percentile price for region orders"""
    query = """
    SELECT
    type_id,
    price
    FROM region_orders
    WHERE is_buy_order = 0
    """

    engine = sa.create_engine(wcmkt_local_url)

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    engine.dispose()
    
    if df.empty:
        logger.warning("No region orders found for 5th percentile calculation")
        return pd.DataFrame(columns=["type_id", "5_perc_price"])
    
    df = df.groupby("type_id")["price"].quantile(0.05).reset_index()
    df.price = df.price.apply(lambda x: round(x, 2))
    df.columns = ["type_id", "5_perc_price"]
    return df


def calculate_region_stats() -> pd.DataFrame:
    """
    Calculate region statistics using deployment_watchlist, region_orders, and region_history.
    Adapted from calculate_market_stats for regional data.
    """
    query = """
    SELECT
    w.type_id,
    w.type_name,
    w.group_name,
    w.category_name,
    w.category_id,
    w.group_id,
    o.min_price,
    o.total_volume_remain,
    h.avg_price,
    h.avg_volume,
    ROUND(CASE
    WHEN h.avg_volume > 0 THEN o.total_volume_remain / h.avg_volume
    ELSE 0
    END, 2) as days_remaining

    FROM deployment_watchlist w

    LEFT JOIN (
    SELECT
        type_id, 
        MIN(price) as min_price,
        SUM(volume_remain) as total_volume_remain
    FROM region_orders
        WHERE is_buy_order = 0
        GROUP BY type_id
    ) AS o
    ON w.type_id = o.type_id
    LEFT JOIN (
    SELECT
        type_id,
        AVG(average) as avg_price,
        AVG(volume) as avg_volume
    FROM region_history
    WHERE date >= DATE('now', '-30 day') AND average > 0 AND volume > 0
    GROUP BY type_id
    ) AS h ON w.type_id = h.type_id
    """

    engine = sa.create_engine(wcmkt_local_url)

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
        logger.info(f"Region stats queried: {df.shape[0]} items")

    engine.dispose()

    if df.empty:
        logger.warning("No region stats data found")
        return pd.DataFrame()

    logger.info(f"Calculating region 5 percentile price")
    df2 = calculate_region_5_percentile_price()

    logger.info(f"Merging 5 percentile price with region stats")
    df = df.merge(df2, on="type_id", how="left")

    logger.info(f"Processing region stats data")
    df.days_remaining = df.days_remaining.apply(lambda x: round(x, 1))
    df = df.rename(columns={"5_perc_price": "price"})
    df["last_update"] = pd.Timestamp.now(tz="UTC")
    
    # Get columns from RegionStats model
    from models import RegionStats
    db_cols = RegionStats.__table__.columns.keys()
    df = df[db_cols]
    
    df["avg_price"] = df["avg_price"].apply(lambda x: round(x, 2))
    df["avg_volume"] = df["avg_volume"].apply(lambda x: round(x, 1))
    df = df.infer_objects()
    df = df.fillna(0)
    df["total_volume_remain"] = df["total_volume_remain"].astype(int)
    
    logger.info(f"Region stats calculated: {df.shape[0]} items")
    return df

def get_deployment_watchlist() -> pd.DataFrame:
    query = """
    SELECT
    *
    FROM deployment_watchlist
    """
    engine = sa.create_engine(wcmkt_local_url)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    engine.dispose()
    return df

def generate_deployment_watchlist(csv_file_path: str = "data/nakah_watchlist_updated.csv"):
    df = pd.read_csv(csv_file_path)
 

    engine = create_engine(wcmkt_local_url)


    with engine.connect() as conn:
        df.to_sql("deployment_watchlist", if_exists="replace", con=conn, index=False)
        stmt = text("SELECT COUNT(*) FROM deployment_watchlist")
        res = conn.execute(stmt)
        data = res.fetchone()[0]
        if data > 0:
            logger.info(f"deployment watchlist updated with {data} items")
        else:
            logger.error("deployment_watchlist updated failed, no data present")

def add_missing_items_to_watchlist():
    missing = [21889, 21888, 21890, 47926, 2629, 16274, 17889, 17888, 17887, 41490, 32014, 16273]

    engine = create_engine(sde_local_url)
    with engine.connect() as conn:
        # Use proper parameter binding for IN clause with SQLite
        from sqlalchemy import bindparam
        stmt = text("SELECT * FROM inv_info WHERE typeID IN :missing").bindparams(bindparam('missing', expanding=True))
        res = conn.execute(stmt, {"missing": missing})
        df = pd.DataFrame(res.fetchall())
        df.columns = res.keys()
        print(df.columns)

    watchlist = get_watchlist()
    inv_cols = ['typeID', 'typeName', 'groupID', 'groupName', 'categoryID',
       'categoryName']
    df = df[inv_cols]
    watchlist_cols = ['type_id', 'type_name', 'group_id', 'group_name', 'category_id', 'category_name']
    df = df.rename(columns=dict(zip(inv_cols, watchlist_cols)))
    df.to_csv("data/watchlist_missing.csv", index=False)
    watchlist = pd.concat([watchlist, df], ignore_index=True)
    watchlist.to_csv("data/watchlist_updated.csv", index=False)
    deploy_watchlist = get_deployment_watchlist()
    deploy_watchlist = pd.concat([deploy_watchlist, df], ignore_index=True)
    deploy_watchlist.to_csv("data/deployment_watchlist_updated.csv", index=False)

def update_watchlist_tables():
    """Update both watchlist and deployment_watchlist tables with missing items"""
    missing = [21889, 21888, 21890, 47926, 2629, 16274, 17889, 17888, 17887, 41490, 32014, 16273]

    # Get missing items from SDE
    engine = create_engine(sde_local_url)
    with engine.connect() as conn:
        from sqlalchemy import bindparam
        stmt = text("SELECT * FROM inv_info WHERE typeID IN :missing").bindparams(bindparam('missing', expanding=True))
        res = conn.execute(stmt, {"missing": missing})
        df = pd.DataFrame(res.fetchall())
        df.columns = res.keys()
    
    # Prepare data for database insertion
    inv_cols = ['typeID', 'typeName', 'groupID', 'groupName', 'categoryID', 'categoryName']
    watchlist_cols = ['type_id', 'type_name', 'group_id', 'group_name', 'category_id', 'category_name']
    df = df[inv_cols]
    df = df.rename(columns=dict(zip(inv_cols, watchlist_cols)))
    
    # Update watchlist table
    engine = create_engine(wcmkt_local_url)
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

if __name__ == "__main__":
    pass
