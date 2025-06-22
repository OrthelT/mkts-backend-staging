
import pandas as pd
import sqlalchemy as sa
from logging_config import configure_logging
from models import MarketStats, Doctrines
import libsql

local_mkt_path = "wcmkt2.db"
local_sde_path = "sde.db"
engine = sa.create_engine(f"sqlite+libsql:///{local_mkt_path}")
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

    engine = sa.create_engine(f"sqlite+libsql:///{local_mkt_path}")

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

    engine = sa.create_engine(f"sqlite+libsql:///{local_mkt_path}", echo="debug")
    logger.info(f"Calculating market stats with {engine}")

    logger.info(f"Querying market stats with {query}")
    logger.info(f"Engine: {engine}")

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
    pd.set_option("display.max_columns", None)
    db_cols = MarketStats.__table__.columns.keys()
    df = df[db_cols]
    df["avg_price"] = df["avg_price"].apply(lambda x: round(x, 2))
    df["avg_volume"] = df["avg_volume"].apply(lambda x: round(x, 1))
    df = df.infer_objects()
    df = df.fillna(0)
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
    engine = sa.create_engine(f"sqlite+libsql:///{local_mkt_path}")

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
    val_cols = Doctrines.__table__.columns.keys()
    col_compare = set(doctrine_stats.columns) - set(val_cols)
    return doctrine_stats


if __name__ == "__main__":
    pass