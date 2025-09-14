import pandas as pd
from pandas.core.internals.blocks import NumpyBlock
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.db.models import MarketStats, RegionHistory, MarketHistory
from mkts_backend.config.config import DatabaseConfig
from mkts_backend.config.esi_config import ESIConfig
from mkts_backend.esi.esi_requests import fetch_region_history
from mkts_backend.utils.utils import (
    add_timestamp,
    add_autoincrement,
    validate_columns,
    convert_datetime_columns,
    get_type_names_from_df,
)
from mkts_backend.db.db_handlers import upsert_database
from mkts_backend.db.db_queries import (
    get_table_length,
    get_remote_status,
    get_system_orders_from_db,
)
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import select, text

esi = ESIConfig("primary")
wcmkt_db = DatabaseConfig("wcmkt")
sde_db = DatabaseConfig("sde")

logger = configure_logging(__name__)

def calculate_5_percentile_price() -> pd.DataFrame:
    query = """
    SELECT
    type_id,
    price
    FROM marketorders
    WHERE is_buy_order = 0
    """
    engine = wcmkt_db.engine
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    conn.close()
    logger.info(f"5 percentile price queried: {df.shape[0]} items")
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
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
        logger.info(f"df: {df}")
        logger.info(f"Market stats queried: {df.shape[0]} items")

    engine.dispose()
    logger.info("Calculating 5 percentile price")
    df2 = calculate_5_percentile_price()
    logger.info("Merging 5 percentile price with market stats")
    df = df.merge(df2, on="type_id", how="left")

    logger.info("Renaming columns")
    df.days_remaining = df.days_remaining.apply(lambda x: round(x, 1))
    df = df.rename(columns={"5_perc_price": "price"})
    df["last_update"] = pd.Timestamp.now(tz="UTC")

    # Fill missing stats from history data
    df = fill_missing_stats_from_history(df)

    # Round numeric columns
    df["avg_price"] = df["avg_price"].apply(lambda x: round(x, 2) if pd.notnull(x) and x > 0 else 0)
    df["avg_volume"] = df["avg_volume"].apply(lambda x: round(x, 1) if pd.notnull(x) and x > 0 else 0)
    df["total_volume_remain"] = df["total_volume_remain"].fillna(0).astype(int)
    df["days_remaining"] = df["days_remaining"].fillna(0)

    # Ensure we have all required database columns
    db_cols = MarketStats.__table__.columns.keys()
    df = df[db_cols]

    logger.info(f"Market stats calculated: {df.shape[0]} items")
    return df

def fill_missing_stats_from_history(stats: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing stats from market history data.
    - When min_price is null: use minimum of average from market history
    - When price is null: use average of average from market history
    - When avg_price/avg_volume is null or no history data: use 0
    """
    # Get all type_ids that have any null values
    null_mask = stats[['min_price', 'price', 'avg_price', 'avg_volume']].isnull().any(axis=1)
    items_with_nulls = stats[null_mask]

    if items_with_nulls.empty:
        return stats

    missing_ids = items_with_nulls["type_id"].unique().tolist()
    # Convert to strings since MarketHistory.type_id is a String field
    missing_ids = [str(id) for id in missing_ids]

    logger.info(f"Missing IDs to query: {missing_ids[:5]}...")  # Log first 5 for debugging

    db = DatabaseConfig("wcmkt")
    engine = db.engine

    try:
        # Check if we have any missing_ids to query
        if not missing_ids:
            logger.warning("No missing IDs to query")
            return stats

        # Query market history using pandas read_sql for easier handling
        # Convert list to comma-separated string for IN clause
        id_list_str = ','.join([f"'{id}'" for id in missing_ids])
        query = f"SELECT type_id, average, volume FROM market_history WHERE type_id IN ({id_list_str})"

        logger.info(f"Querying history for {len(missing_ids)} type_ids")

        try:
            with engine.connect() as conn:
                history_df = pd.read_sql_query(query, conn)
        except Exception as e:
            logger.error(f"Error in SQL query: {e}")
            raise

        logger.info(f"Found {len(history_df)} history records")

        if history_df.empty:
            # No history data available, set all nulls to 0
            stats.loc[null_mask, 'avg_price'] = 0
            stats.loc[null_mask, 'avg_volume'] = 0
            stats.loc[null_mask, 'min_price'] = 0
            stats.loc[null_mask, 'price'] = 0
            return stats

        try:
            # Process each type_id individually to avoid aggregation issues
            for type_id in missing_ids:
                type_history = history_df[history_df['type_id'] == type_id]

                if type_history.empty:
                    continue

                # Calculate statistics for this type_id
                min_avg = type_history['average'].min()
                mean_avg = type_history['average'].mean()
                mean_vol = type_history['volume'].mean()

                # Apply the rules for this type_id
                type_mask = stats['type_id'].astype(str) == type_id

                # 1. min_price null -> use minimum of average from market history
                if stats.loc[type_mask, 'min_price'].isnull().any():
                    stats.loc[type_mask & stats['min_price'].isnull(), 'min_price'] = min_avg

                # 2. price null -> use average of average from market history
                if stats.loc[type_mask, 'price'].isnull().any():
                    stats.loc[type_mask & stats['price'].isnull(), 'price'] = mean_avg

                # 3. avg_price null -> use mean_average or 0 if no history
                if stats.loc[type_mask, 'avg_price'].isnull().any():
                    stats.loc[type_mask & stats['avg_price'].isnull(), 'avg_price'] = mean_avg

                # 4. avg_volume null -> use mean_volume or 0 if no history
                if stats.loc[type_mask, 'avg_volume'].isnull().any():
                    stats.loc[type_mask & stats['avg_volume'].isnull(), 'avg_volume'] = mean_vol
        except Exception as e:
            logger.error(f"Error in processing type_ids: {e}")
            raise

        try:
            # For items with no history data at all, set to 0
            no_history_mask = stats[['min_price', 'price', 'avg_price', 'avg_volume']].isnull().any(axis=1)
            stats.loc[no_history_mask, ['min_price', 'price', 'avg_price', 'avg_volume']] = 0
        except Exception as e:
            logger.error(f"Error in final null handling: {e}")
            raise

        logger.info(f"Filled missing stats for {len(missing_ids)} items from market history")

    except Exception as e:
        logger.error(f"Error filling missing stats from history: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        # Fallback: set all nulls to 0
        stats.loc[null_mask, ['min_price', 'price', 'avg_price', 'avg_volume']] = 0

    finally:
        engine.dispose()

    return stats



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
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        doctrine_stats = pd.read_sql_query(doctrine_query, conn)
        market_stats = pd.read_sql_query(stats_query, conn)
    doctrine_stats = doctrine_stats.drop(columns=[
        "hulls", "fits_on_mkt", "total_stock", "avg_vol", "days", "timestamp"
    ])
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
    return doctrine_stats

def process_system_orders(system_id: int) -> pd.DataFrame:
    df = get_system_orders_from_db(system_id)
    df = not df['is_buy_order']
    nakah_mkt = 60014068
    nakah_df = df[df.location_id == nakah_mkt].reset_index(drop=True)
    nakah_df = nakah_df[["price", "type_id", "volume_remain"]]
    nakah_df = (
        nakah_df.groupby("type_id").agg({"price": lambda x: x.quantile(0.05), "volume_remain": "sum"}).reset_index()
    )
    nakah_ids = nakah_df["type_id"].unique().tolist()
    type_names = get_type_names_from_df(nakah_ids)
    nakah_df = nakah_df.merge(type_names, on="type_id", how="left")
    nakah_df = nakah_df[["type_id", "type_name", "group_name", "category_name", "price", "volume_remain"]]
    nakah_df['timestamp'] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    nakah_df.to_csv("nakah_stats.csv", index=False)
    return nakah_df

def process_region_history(watchlist: pd.DataFrame):
    region_history = fetch_region_history(watchlist)
    valid_history_columns = RegionHistory.__table__.columns.keys()

    # Check if region_history is in the right format
    if isinstance(region_history, list) and len(region_history) > 0:
        if isinstance(region_history[0], dict):
            history_df = pd.DataFrame(region_history)
        else:
            # Convert to list of dicts if needed
            history_df = pd.DataFrame.from_records(region_history)
    else:
        # Create empty DataFrame with correct columns
        history_df = pd.DataFrame(columns=valid_history_columns)
    history_df = add_timestamp(history_df)
    history_df = add_autoincrement(history_df)
    history_df = validate_columns(history_df, valid_history_columns)
    history_df = convert_datetime_columns(history_df, ['date'])
    history_df.infer_objects()
    history_df.fillna(0)

    try:
        upsert_database(RegionHistory, history_df)
    except Exception as e:
        logger.error(f"history data update failed: {e}")

    status = get_remote_status()['market_history']
    if status > 0:
        logger.info(f"History updated:{get_table_length('market_history')} items")
        print(f"History updated:{get_table_length('market_history')} items")
    else:
        logger.error("Failed to update market history")


if __name__ == "__main__":
    pass