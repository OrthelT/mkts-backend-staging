import requests
import json
import time
from sqlalchemy import select, text
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from config.logging_config import configure_logging
from db.models import RegionOrders
import pandas as pd
from utils import get_type_names, get_type_name
from db.db_handlers import add_region_history
from millify import millify
from db.models import RegionHistory
from config import DatabaseConfig, ESIConfig
from jita import get_jita_prices_df
from utils import add_timestamp, add_autoincrement, validate_columns, convert_datetime_columns
from db.db_handlers import upsert_remote_database, get_remote_status, get_table_length


logger = configure_logging(__name__)

"""
This module is a legacy module used to process system orders and calculate market metrics
for Winter Coalition's last deployment to Nakah. It is no longer used in the current deployment, but is maintained here for reference and future use.

It is used to calculate the total market value of a system, the total ship count,
and the total number of ships on the market.
"""

def get_nakah_watchlist(esi: ESIConfig):
    watchlist = esi.get_watchlist()
    return watchlist

def fetch_region_orders(region_id: int, order_type: str = 'sell') -> list[dict]:
    """
    Get all orders for a given region and order type
    Args:
        region_id: int
        order_type: str (sell, buy, all)
    Returns:
        list of order dicts
    """
    orders = []
    max_pages = 1
    page = 1
    error_count = 0
    logger.info(f"Getting orders for region {region_id} with order type {order_type}")
    status_codes = {}
    begin_time = time.time()

    while page <= max_pages:
        status_code = None

        headers = {
            'User-Agent': 'wcmkts_backend/1.0, orthel.toralen@gmail.com, (https://github.com/OrthelT/wcmkts_backend)',
            'Accept': 'application/json',
        }
        base_url = f"https://esi.evetech.net/latest/markets/{region_id}/orders/?datasource=tranquility&order_type={order_type}&page={page}"
        start_time = time.time()
        try:
            response = requests.get(base_url, headers=headers, timeout=10)
            elapsed = millify(response.elapsed.total_seconds(), precision=2)
            status_code = response.status_code
        except requests.exceptions.Timeout as TimeoutError:
            print(TimeoutError)
            elapsed = millify(time.time() - start_time, precision=2)
            logger.error(f"Timeout: {page} of {max_pages} | {elapsed}s")
        except requests.exceptions.ConnectionError as ConnectionError:
            print(ConnectionError)
            elapsed = millify(time.time() - start_time, precision=2)
            logger.error(f"Connection Error: {page} of {max_pages} | {elapsed}s")
        except requests.exceptions.RequestException as RequestException:
            print(RequestException)
            elapsed = millify(time.time() - start_time, precision=2)
            logger.error(f"Request Error: {page} of {max_pages} | {elapsed}s")

        if status_code and status_code != 200:
            logger.error(f"page {page} of {max_pages} | status: {status_code} | {elapsed}s")
            error_count += 1
            if error_count > 5:
                print("error", status_code)
                logger.error(f"Error: {status_code}")
                raise Exception(f"Too many errors: {error_count}")
            time.sleep(1)
            continue
        elif status_code == 200:
            logger.info(f"page {page} of {max_pages} | status: {status_code} | {elapsed}s")
        else:
            # Handle case where response failed (timeout, connection error, etc.)
            logger.error(f"page {page} of {max_pages} | request failed | {elapsed}s")
            error_count += 1
            if error_count > 5:
                logger.error(f"Too many errors: {error_count}")
                raise Exception(f"Too many errors: {error_count}")
            time.sleep(1)
            continue


        # Only process response if we have a valid status code
        if status_code == 200:
            error_remain = response.headers.get('X-Error-Limit-Remain')
            if error_remain == '0':
                logger.critical(f"Too many errors: {error_count}")
                raise Exception(f"Too many errors: {error_count}")

            if response.headers.get('X-Pages'):
                max_pages = int(response.headers.get('X-Pages'))
            else:
                max_pages = 1

            order_page = response.json()
        else:
            # Skip processing this page due to error
            continue


        if order_page == []:
            logger.info("No more orders found")
            logger.info("--------------------------------\n\n")
            return orders
        else:
            for order in order_page:
                orders.append(order)

            page += 1
    logger.info(f"{len(orders)} orders fetched in {millify(time.time() - begin_time, precision=2)}s | {millify(len(orders)/(time.time() - begin_time), precision=2)} orders/s")
    logger.info("--------------------------------\n\n")
    return orders

def get_region_orders_from_db(region_id: int, system_id: int, db: DatabaseConfig) -> pd.DataFrame:
    """
    Get all orders for a given region and order type
    Args:
        region_id: int
        order_type: str (sell, buy, all)
    Returns:
        pandas DataFrame
    """
    stmt = select(RegionOrders).where(RegionOrders.system_id == system_id)

    engine = db.engine
    session = Session(bind=engine)
    result = session.scalars(stmt)
    orders_data = []
    for order in result:
        orders_data.append({
            'order_id': order.order_id,
            'duration': order.duration,
            'is_buy_order': order.is_buy_order,
            'issued': order.issued,
            'location_id': order.location_id,
            'min_volume': order.min_volume,
            'price': order.price,
            'range': order.range,
            'system_id': order.system_id,
            'type_id': order.type_id,
            'volume_remain': order.volume_remain,
            'volume_total': order.volume_total
        })

    session.close()
    return pd.DataFrame(orders_data)

def update_region_orders(region_id: int, order_type: str = 'sell') -> pd.DataFrame:
    """
    Fetch region orders from the database
    Args:
        region_id: int
        order_type: str (sell, buy, all)
    Returns:
        pandas DataFrame
    """
    orders = fetch_region_orders(region_id, order_type)
    engine = DatabaseConfig("wcmkt2").engine
    session = Session(bind=engine)

    # Clear existing orders
    session.query(RegionOrders).delete()
    session.commit()
    session.expunge_all()  # Clear all objects from identity map
    session.close()
    time.sleep(1)
    session = Session(bind=engine)  # Create a fresh session

    # Convert API response dicts to RegionOrders model instances
    for order_data in orders:
        # Convert the API response to match our model fields
        region_order = RegionOrders(
            order_id=order_data['order_id'],
            duration=order_data['duration'],
            is_buy_order=order_data['is_buy_order'],
            issued=datetime.fromisoformat(order_data['issued'].replace('Z', '+00:00')),
            location_id=order_data['location_id'],
            min_volume=order_data['min_volume'],
            price=order_data['price'],
            range=order_data['range'],
            system_id=order_data['system_id'],
            type_id=order_data['type_id'],
            volume_remain=order_data['volume_remain'],
            volume_total=order_data['volume_total']
        )
        session.add(region_order)

    session.commit()
    session.close()

    return pd.DataFrame(orders)

def get_system_orders_from_db(system_id: int) -> pd.DataFrame:
    """
    Get all orders for a given system
    Args:
        system_id: int
    Returns:
        pandas DataFrame
    """
    stmt = select(RegionOrders).where(RegionOrders.system_id == system_id)
    engine = DatabaseConfig("wcmkt2").engine
    session = Session(bind=engine)
    result = session.scalars(stmt)

    # Convert SQLAlchemy objects to dictionaries for DataFrame
    orders_data = []
    for order in result:
        orders_data.append({
            'order_id': order.order_id,
            'duration': order.duration,
            'is_buy_order': order.is_buy_order,
            'issued': order.issued,
            'location_id': order.location_id,
            'min_volume': order.min_volume,
            'price': order.price,
            'range': order.range,
            'system_id': order.system_id,
            'type_id': order.type_id,
            'volume_remain': order.volume_remain,
            'volume_total': order.volume_total
        })

    session.close()
    return pd.DataFrame(orders_data)

def process_system_orders(system_id: int) -> pd.DataFrame:
    df = get_system_orders_from_db(system_id)
    df = df[df['is_buy_order'] == False]
    df2 = df.copy()
    nakah_mkt = 60014068
    nakah_df = df[df.location_id == nakah_mkt].reset_index(drop=True)
    nakah_df = nakah_df[["price","type_id","volume_remain"]]
    nakah_df = nakah_df.groupby("type_id").agg({"price": lambda x: x.quantile(0.05), "volume_remain": "sum"}).reset_index()
    nakah_ids = nakah_df["type_id"].unique().tolist()
    type_names = get_type_names(nakah_ids)
    nakah_df = nakah_df.merge(type_names, on="type_id", how="left")
    nakah_df = nakah_df[["type_id", "type_name", "group_name", "category_name", "price", "volume_remain"]]
    nakah_df['timestamp'] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    nakah_df.to_csv("nakah_stats.csv", index=False)
    return nakah_df


def calculate_total_market_value(market_data: pd.DataFrame) -> float:
    """
    Calculate the total market value from process_system_orders output

    Args:
        market_data: DataFrame from process_system_orders containing price and volume_remain columns

    Returns:
        Total market value as float
    """
    if market_data is None or market_data.empty:
        logger.warning("No market data provided for value calculation")
        return 0.0

    # Filter out Blueprint and Skill categories
    filtered_data = market_data[
        (~market_data['category_name'].isin(['Blueprint', 'Skill']))
    ].copy()

    if filtered_data.empty:
        logger.warning("No market data after filtering out Blueprint and Skill categories")
        print("No market data after filtering out Blueprint and Skill categories")
        return 0.0
    # Calculate total value for each item (price * volume_remain)
    filtered_data['total_value'] = filtered_data['price'] * filtered_data['volume_remain']
    # Sum all individual totals to get overall market value
    total_market_value = filtered_data['total_value'].sum()
    logger.info(f"Total market value calculated: {millify(total_market_value, precision=2)} ISK")
    print(f"Total market value calculated: {millify(total_market_value, precision=2)} ISK")
    return total_market_value


def get_system_market_value(system_id: int) -> float:
    """
    Convenience function to get total market value for a system

    Args:
        system_id: System ID to calculate market value for

    Returns:
        Total market value as float
    """
    market_data = process_system_orders(system_id)
    return calculate_total_market_value(market_data)


def calculate_total_ship_count(market_data: pd.DataFrame) -> int:
    """
    Calculate the total number of ships on the market

    Args:
        market_data: DataFrame from process_system_orders containing category_name and volume_remain columns

    Returns:
        Total ship count as int
    """
    if market_data is None or market_data.empty:
        logger.warning("No market data provided for ship count calculation")
        print("No market data provided for ship count calculation")
        return 0

    # Filter for ships only and sum volume_remain
    ships_data = market_data[market_data['category_name'] == 'Ship']
    total_ship_count = ships_data['volume_remain'].sum()

    logger.info(f"Total ships on market: {total_ship_count:,}")
    print(f"Total ships on market: {total_ship_count:,}")
    return int(total_ship_count)


def get_system_ship_count(system_id: int) -> int:
    """
    Convenience function to get total ship count for a system

    Args:
        system_id: System ID to calculate ship count for

    Returns:
        Total ship count as int
    """
    market_data = process_system_orders(system_id)
    return calculate_total_ship_count(market_data)

def get_region_history(type_ids: list[int])->list[dict]:
    engine = DatabaseConfig("wcmkt2").engine
    print("engine created")
    session = Session(bind=engine)
    print("session created")
    stmt = select(RegionHistory).where(RegionHistory.type_id.in_(type_ids))
    print("stmt created")
    result = session.scalars(stmt)
    print("result created")
    history = []
    for item in result:
        print(item)
        # Convert the RegionHistory object to a dictionary format
        history_data = {
            'date': item.date.strftime("%Y-%m-%d"),
            'average': item.average,
            'highest': item.highest,
            'lowest': item.lowest,
            'order_count': item.order_count,
            'volume': item.volume
        }
        history.append({item.type_id: [history_data]})
    session.close()
    return history

def get_system_orders(system_id: int, db: DatabaseConfig) -> pd.DataFrame:
    engine = db.engine
    session = Session(bind=engine)
    stmt = select(RegionOrders).where(RegionOrders.system_id == system_id)
    result = session.execute(stmt)
    return result.all()

def get_orders_stats(system_id: int) -> pd.DataFrame:
    od = []
    orders = get_system_orders(system_id, DatabaseConfig("wcmkt2"))
    for o in orders:
        row = o._asdict()
        item = row["RegionOrders"]
        data = {'order_id': item.order_id, 'volume_remain': item.volume_remain, 'price': item.price, 'type_id': item.type_id, 'system_id': item.system_id, 'buy_order': item.is_buy_order, 'duration': item.duration, 'issued': item.issued, 'location_id': item.location_id}
        od.append(data)
    df = pd.DataFrame(od)

    df2 = not df['buy_order']
    df2 = df2.groupby('type_id').agg({'volume_remain': 'sum', 'price': 'mean'}).reset_index()
    types_df = get_type_names(df2['type_id'].tolist())
    df3 = df2.merge(types_df, on='type_id', how='left')
    jita_prices_df = get_jita_prices_df(df3['type_id'].tolist())
    df3 = df3.merge(jita_prices_df, on='type_id', how='left')


    return df3


def process_region_history(watchlist: pd.DataFrame):
    region_history = fetch_region_history(watchlist)
    valid_history_columns = RegionHistory.__table__.columns.keys()
    history_df = pd.DataFrame.from_records(region_history)
    history_df = add_timestamp(history_df)
    history_df = add_autoincrement(history_df)
    history_df = validate_columns(history_df, valid_history_columns)

    history_df = convert_datetime_columns(history_df,['date'])

    history_df.infer_objects()
    history_df.fillna(0)

    try:
        upsert_remote_database(RegionHistory, history_df)
    except Exception as e:
        logger.error(f"history data update failed: {e}")

    status = get_remote_status()['market_history']
    if status > 0:
        logger.info(f"History updated:{get_table_length('market_history')} items")
        print(f"History updated:{get_table_length('market_history')} items")
    else:
        logger.error("Failed to update market history")

def add_region_history(history: list[dict]):
    timestamp = datetime.now(timezone.utc)
    timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    db = DatabaseConfig("wcmkt3")
    engine = db.engine
    session = Session(bind=engine)

    with session.begin():
        session.query(RegionHistory).delete()

        for item in history:
            for type_id, history in item.items():
                print(f"Processing type_id: {type_id}, {get_type_name(type_id)}")
                for record in history:
                    date = datetime.strptime(record["date"], "%Y-%m-%d")
                    order = RegionHistory(type_id=type_id, average=record["average"], date=date, highest=record["highest"], lowest=record["lowest"], order_count=record["order_count"], volume=record["volume"], timestamp=datetime.now(timezone.utc))
                    session.add(order)
        session.commit()
        session.close()
        engine.dispose()

def get_region_history()-> pd.DataFrame:
    db = DatabaseConfig("wcmkt3")
    engine = db.engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM region_history")
        result = conn.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    conn.close()
    return df

def get_region_deployment_history(deployment_date: datetime) -> pd.DataFrame:
    """
    Get region history data after a specified deployment date.

    Args:
        deployment_date: datetime object representing the deployment date

    Returns:
        pandas DataFrame containing region history records after the deployment date
    """
    df = get_region_history()

    if df.empty:
        print("No region history data found")
        return df

    # Convert the date column to datetime if it's not already
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])

        # Filter records after the deployment date
        filtered_df = df[df['date'] >= deployment_date].copy()

        # Sort by date for better readability
        filtered_df = filtered_df.sort_values('date')

        print(f"Found {len(filtered_df)} records after {deployment_date.strftime('%Y-%m-%d')}")
        print(f"Date range: {filtered_df['date'].min()} to {filtered_df['date'].max()}")

        return filtered_df
    else:
        print("No 'date' column found in region history data")
        return df


def fetch_region_history(watchlist: pd.DataFrame) -> list[dict]:
    esi = ESIConfig("secondary")
    MARKET_HISTORY_URL = esi.market_history_url
    deployment_reg_id = esi.region_id

    logger.info("Fetching history")
    if watchlist is None or watchlist.empty:
        logger.error("No watchlist provided or watchlist is empty")
        return None
    else:
        logger.info("Watchlist found")
        print(f"Watchlist found: {len(watchlist)} items")

    type_ids = watchlist["type_id"].tolist()
    logger.info(f"Fetching history for {len(type_ids)} types")

    headers = esi.headers()

    history = []

    watchlist_length = len(watchlist)
    for i, type_id in enumerate(type_ids):
        item_name = watchlist[watchlist["type_id"] == type_id]["type_name"].values[0]
        try:
            url = f"{MARKET_HISTORY_URL}"

            querystring = {"type_id": str(type_id)}

            print(
                f"\rFetching history for ({i + 1}/{watchlist_length})",
                end="",
                flush=True,
            )
            response = requests.get(url, headers=headers, params=querystring)
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
        with open("region_history.json", "w") as f:
            json.dump(history, f)
        return history

    else:
        logger.error("No history records found")
        return None

if __name__ == "__main__":
    pass
