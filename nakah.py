import requests
import os
import json
import time
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session
from proj_config import wcmkt_url, db_path
from datetime import datetime, timezone
from logging_config import configure_logging
from models import RegionOrders, Base
import pandas as pd
logger = configure_logging(__name__)
from utils import get_type_names
sys_id = 30000072
reg_id = 10000001
from millify import millify


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
    
    while page <= max_pages:
        logger.info(f"Getting page {page} of {max_pages}")
        headers = {
            'User-Agent': 'wcmkts_new/1.0, orthel.toralen@gmail.com, (https://github.com/OrthelT/wcmkts_new)',
            'Accept': 'application/json',
        }
        base_url = f"https://esi.evetech.net/latest/markets/{region_id}/orders/?datasource=tranquility&order_type={order_type}&page={page}"
        response = requests.get(base_url)
        logger.info(f"Response: {response.status_code}")
        if response.status_code != 200:
            error_count += 1
            if error_count > 3:
                raise Exception(f"Too many errors: {error_count}")
            logger.error(f"Error: {response.status_code}")
            time.sleep(1)
            continue
        
        error_remain = response.headers.get('X-Error-Limit-Remain')
        if error_remain == '0':
            raise Exception(f"Too many errors: {error_count}")
    
        if response.headers.get('X-Pages'):
            max_pages = int(response.headers.get('X-Pages'))
        else:
            max_pages = 1
        
        order_page = response.json()


        if order_page == []:
            break
        else:
            for order in order_page:
                orders.append(order)

            page += 1
    return orders

def get_region_orders_from_db(region_id: int) -> pd.DataFrame:
    """
    Get all orders for a given region and order type
    Args:
        region_id: int
        order_type: str (sell, buy, all)
    Returns:
        pandas DataFrame
    """
    stmt = select(RegionOrders)

    engine = create_engine(wcmkt_url)
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
    engine = create_engine(wcmkt_url)
    session = Session(bind=engine)
    
    # Clear existing orders
    session.query(RegionOrders).delete()
    session.commit()
    
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
    engine = create_engine(wcmkt_url)
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
    nakah = 60014068
    nakah_df = df[df.location_id == nakah].reset_index(drop=True)
    nakah_df = nakah_df[["price","type_id","volume_remain"]]
    nakah_df = nakah_df.groupby("type_id").agg({"price": "mean", "volume_remain": "sum"}).reset_index()
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


if __name__ == "__main__":
    orders = fetch_region_orders(reg_id, "sell")
    print(orders)
    print(len(orders))