import requests
import os
import json
import time
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session
from proj_config import wcmkt_url, db_path
from datetime import datetime
from logging_config import configure_logging
from models import RegionOrders, Base
import pandas as pd
logger = configure_logging(__name__)

sys_id = 30000072
reg_id = 10000001


def get_region_orders(region_id: int, order_type: str = 'sell') -> list[dict]:
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
    orders = get_region_orders(region_id, order_type)
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



if __name__ == "__main__":
    pass



 