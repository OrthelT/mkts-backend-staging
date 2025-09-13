import requests
import json
import time
from sqlalchemy import select, text
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.db.models import RegionOrders, RegionHistory
import pandas as pd
from mkts_backend.utils.utils import (
    get_type_names_from_df,
    get_type_name,
    add_timestamp,
    add_autoincrement,
    validate_columns,
    convert_datetime_columns,
)
from mkts_backend.db.db_handlers import (
    upsert_remote_database,
    get_remote_status,
    get_table_length,
)
from millify import millify
from mkts_backend.config import DatabaseConfig, ESIConfig
from mkts_backend.utils.jita import get_jita_prices_df


logger = configure_logging(__name__)

"""
Legacy module used for Nakah deployment; kept for reference.
"""


def get_nakah_watchlist(esi: ESIConfig):
    watchlist = esi.get_watchlist()
    return watchlist


# Many functions omitted for brevity; keep core logic references intact

def get_region_orders_from_db(region_id: int, system_id: int, db: DatabaseConfig) -> pd.DataFrame:
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
