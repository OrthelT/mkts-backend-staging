from sqlalchemy import String, Integer, DateTime, Float, Boolean, create_engine, event
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker, Session
import libsql
from proj_config import wcmkt_url, db_path
from utils import get_type_name
from datetime import datetime, timezone
import json
import sqlalchemy as sa

class Base(DeclarativeBase):
    pass

class MarketStats(Base):
    __tablename__ = "marketstats"
    type_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    total_volume_remain: Mapped[int] = mapped_column(Integer)
    min_price: Mapped[float] = mapped_column(Float)
    price: Mapped[float] = mapped_column(Float)
    avg_price: Mapped[float] = mapped_column(Float)
    avg_volume: Mapped[float] = mapped_column(Float)
    group_id: Mapped[int] = mapped_column(Integer)
    type_name: Mapped[str] = mapped_column(String)
    group_name: Mapped[str] = mapped_column(String)
    category_id: Mapped[int] = mapped_column(Integer)
    category_name: Mapped[str] = mapped_column(String)
    days_remaining: Mapped[float] = mapped_column(Float)
    last_update: Mapped[DateTime] = mapped_column(DateTime)

    def __repr__(self) -> str:
        f"""marketstats(type_id={self.type_id!r}, 
        total_volume_remain={self.total_volume_remain!r}, 
        min_price={self.min_price!r},
        price={self.price!r},
        avg_price={self.avg_price!r},
        avg_volume={self.avg_volume!r},
        group_id={self.group_id!r},
        type_name={self.type_name!r},   
        group_name={self.group_name!r},
        category_id={self.category_id!r},
        category_name={self.category_name!r},
        days_remaining={self.days_remaining!r},
        last_update={self.last_update!r}
        """


class MarketOrders(Base):
    __tablename__ = "marketorders"
    order_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    is_buy_order: Mapped[bool] = mapped_column(Boolean, nullable=True)
    type_id: Mapped[int] = mapped_column(Integer, nullable=True)
    type_name: Mapped[str] = mapped_column(String, nullable=True)
    duration: Mapped[int] = mapped_column(Integer, nullable=True)
    issued: Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    price: Mapped[float] = mapped_column(Float, nullable=True)
    volume_remain: Mapped[int] = mapped_column(Integer, nullable=True)

    def __repr__(self) -> str:
        f"""marketorders(
        order_id={self.order_id!r},
        is_buy_order={self.is_buy_order!r},
        type_id={self.type_id!r},
        type_name={self.type_name!r},
        duration={self.duration!r},
        issued={self.issued!r},
        price={self.price!r},
        volume_remain={self.volume_remain!r},
        )"""


class MarketHistory(Base):
    __tablename__ = "market_history"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[DateTime] = mapped_column(DateTime)
    type_name: Mapped[str] = mapped_column(String(100))
    type_id: Mapped[str] = mapped_column(String(10))
    average: Mapped[float] = mapped_column(Float)
    volume: Mapped[int] = mapped_column(Integer)
    highest: Mapped[float] = mapped_column(Float)
    lowest: Mapped[float] = mapped_column(Float)
    order_count: Mapped[int] = mapped_column(Integer)
    timestamp: Mapped[DateTime] = mapped_column(DateTime)

    def __repr__(self) -> str:
        f"""market_history(
        date={self.date!r},
        type_name={self.type_name!r},
        type_id={self.type_id!r},
        average={self.average!r},
        volume={self.volume!r},
        highest={self.highest!r},
        lowest={self.lowest!r},
        order_count={self.order_count!r},
        timestamp={self.timestamp!r}
        )"""


class Doctrines(Base):
    __tablename__ = "doctrines"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fit_id: Mapped[int] = mapped_column(Integer)
    ship_id: Mapped[int] = mapped_column(Integer)
    ship_name: Mapped[str] = mapped_column(String)
    hulls: Mapped[int] = mapped_column(Integer)
    type_id: Mapped[int] = mapped_column(Integer)
    type_name: Mapped[str] = mapped_column(String)
    fit_qty: Mapped[int] = mapped_column(Integer)
    fits_on_mkt: Mapped[float] = mapped_column(Float)
    total_stock: Mapped[int] = mapped_column(Integer)
    price: Mapped[float] = mapped_column(Float)
    avg_vol: Mapped[float] = mapped_column(Float)
    days: Mapped[float] = mapped_column(Float)
    group_id: Mapped[int] = mapped_column(Integer)
    group_name: Mapped[str] = mapped_column(String)
    category_id: Mapped[int] = mapped_column(Integer)
    category_name: Mapped[str] = mapped_column(String)
    timestamp: Mapped[DateTime] = mapped_column(DateTime)

    def __repr__(self) -> str:
        f"""doctrines(
        fit_id={self.fit_id!r},
        ship_id={self.ship_id!r},
        ship_name={self.ship_name!r},
        hulls={self.hulls!r},
        type_id={self.type_id!r},
        type_name={self.type_name!r},
        fit_qty={self.fit_qty!r},
        fits_on_mkt={self.fits_on_mkt!r},
        total_stock={self.total_stock!r},
        price={self.price!r},
        avg_vol={self.avg_vol!r},
        days={self.days!r},
        group_id={self.group_id!r},
        group_name={self.group_name!r},
        category_id={self.category_id!r},
        category_name={self.category_name!r},
        timestamp={self.timestamp!r}
        )"""


class ShipTargets(Base):
    __tablename__ = "ship_targets"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fit_id: Mapped[int] = mapped_column(Integer)
    fit_name: Mapped[str] = mapped_column(String)
    ship_id: Mapped[int] = mapped_column(Integer)
    ship_name: Mapped[str] = mapped_column(String)
    ship_target: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[DateTime] = mapped_column(DateTime)

    def __repr__(self) -> str:
        f"""ship_targets(
        fit_id={self.fit_id!r},
        fit_name={self.fit_name!r},
        ship_id={self.ship_id!r},
        ship_name={self.ship_name!r},
        ship_target={self.ship_target!r},
        created_at={self.created_at!r}
        )"""

class DoctrineMap(Base):
    __tablename__ = "doctrine_map"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doctrine_id: Mapped[int] = mapped_column(Integer)
    fitting_id: Mapped[int] = mapped_column(Integer)

    def __repr__(self) -> str:
        f"""doctrine_map(
        doctrine_id={self.doctrine_id!r},
        fitting_id={self.fitting_id!r}
        )"""

class Watchlist(Base):
    __tablename__ = "watchlist"
    type_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    group_id: Mapped[int] = mapped_column(Integer)
    type_name: Mapped[str] = mapped_column(String)
    group_name: Mapped[str] = mapped_column(String)
    category_id: Mapped[int] = mapped_column(Integer)
    category_name: Mapped[str] = mapped_column(String)

    def __repr__(self) -> str:
        f"""watchlist(
        type_id={self.type_id!r},
        group_id={self.group_id!r},
        type_name={self.type_name!r},
        group_name={self.group_name!r},
        category_id={self.category_id!r},
        category_name={self.category_name!r}
        )"""

class NakahWatchlist(Base):
    __tablename__ = "nakah_watchlist"
    type_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    type_name: Mapped[str] = mapped_column(String)

    def __repr__(self) -> str:
        f"""nakah_watchlist(
        type_id={self.type_id!r},
        type_name={self.type_name!r}
        )"""

class DoctrineInfo(Base):
    __tablename__ = "doctrine_info"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doctrine_id: Mapped[int] = mapped_column(Integer)
    doctrine_name: Mapped[str] = mapped_column(String)

class DoctrineFits(Base):
    __tablename__ = "doctrine_fits"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doctrine_name: Mapped[str] = mapped_column(String)
    fit_name: Mapped[str] = mapped_column(String)
    ship_type_id: Mapped[int] = mapped_column(Integer)
    doctrine_id: Mapped[int] = mapped_column(Integer)
    fit_id: Mapped[int] = mapped_column(Integer)
    ship_name: Mapped[str] = mapped_column(String)
    target: Mapped[int] = mapped_column(Integer)

    def __repr__(self) -> str:
        return f"""doctrine_fits(
        id={self.id!r},
        doctrine_name={self.doctrine_name!r},
        fit_name={self.fit_name!r},
        ship_type_id={self.ship_type_id!r},
        doctrine_id={self.doctrine_id!r},
        fit_id={self.fit_id!r},
        ship_name={self.ship_name!r},
        target={self.target!r}
        )"""

class RegionOrders(Base):
    __tablename__ = "region_orders"
    order_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    duration:  Mapped[int] = mapped_column(Integer)
    is_buy_order: Mapped[bool] = mapped_column(Boolean)
    issued: Mapped[DateTime] = mapped_column(DateTime)
    location_id: Mapped[int] = mapped_column(Integer)
    min_volume: Mapped[int] = mapped_column(Integer)
    price: Mapped[float] = mapped_column(Float)
    range: Mapped[str] = mapped_column(String)
    system_id: Mapped[int] = mapped_column(Integer)
    type_id: Mapped[int] = mapped_column(Integer)
    volume_remain: Mapped[int] = mapped_column(Integer)
    volume_total: Mapped[int] = mapped_column(Integer)

    @property
    def resolved_type_name(self) -> str:
        """
        Returns the type name for the current type_id using the get_type_name utility function.
        """
        return get_type_name(self.type_id)

    def __repr__(self) -> str:
        return f"""region_orders(
        order_id={self.order_id!r},
        duration={self.duration!r},
        is_buy_order={self.is_buy_order!r},
        issued={self.issued!r},
        location_id={self.location_id!r},
        min_volume={self.min_volume!r},
        price={self.price!r},
        range={self.range!r},
        system_id={self.system_id!r},
        type_id={self.type_id!r},
        volume_remain={self.volume_remain!r},
        volume_total={self.volume_total!r}
        )"""

class RegionHistory(Base):
    __tablename__ = "region_history"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    type_id: Mapped[int] = mapped_column(Integer)
    average: Mapped[float] = mapped_column(Float)
    date: Mapped[DateTime] = mapped_column(DateTime)
    highest: Mapped[float] = mapped_column(Float)
    lowest: Mapped[float] = mapped_column(Float)
    order_count: Mapped[int] = mapped_column(Integer)
    volume: Mapped[int] = mapped_column(Integer)
    timestamp: Mapped[DateTime] = mapped_column(DateTime)
    type_name: Mapped[str] = mapped_column(String)
    @property
    def resolved_type_name(self) -> str:
        """
        Returns the type name for the current type_id using the get_type_name utility function.
        """
        return get_type_name(self.type_id)


    def __repr__(self) -> str:
        return f"""region_history(
        type_id={self.type_id!r},
        type_name={self.type_name!r},
        average={self.average!r},
        date={self.date!r},
        highest={self.highest!r},
        lowest={self.lowest!r},
        order_count={self.order_count!r},
        volume={self.volume!r},
        timestamp={self.timestamp!r}
        )"""

class DeploymentWatchlist(Base):
    __tablename__ = "deployment_watchlist"
    type_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    group_id: Mapped[int] = mapped_column(Integer, nullable=True)
    type_name: Mapped[str] = mapped_column(String, nullable=True)
    group_name: Mapped[str] = mapped_column(String, nullable=True)
    category_id: Mapped[int] = mapped_column(Integer, nullable=True)
    category_name: Mapped[str] = mapped_column(String, nullable=True)

    def __repr__(self) -> str:
        f"""deployment_watchlist(
        type_id={self.type_id!r},
        group_id={self.group_id!r},
        type_name={self.type_name!r},
        group_name={self.group_name!r},
        category_id={self.category_id!r},
        category_name={self.category_name!r}
        )"""

class RegionStats(Base):
    __tablename__ = "region_stats"
    type_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    total_volume_remain: Mapped[int] = mapped_column(Integer)
    min_price: Mapped[float] = mapped_column(Float)
    price: Mapped[float] = mapped_column(Float)
    avg_price: Mapped[float] = mapped_column(Float)
    avg_volume: Mapped[float] = mapped_column(Float)
    group_id: Mapped[int] = mapped_column(Integer)
    type_name: Mapped[str] = mapped_column(String)
    group_name: Mapped[str] = mapped_column(String)
    category_id: Mapped[int] = mapped_column(Integer)
    category_name: Mapped[str] = mapped_column(String)
    days_remaining: Mapped[float] = mapped_column(Float)
    last_update: Mapped[DateTime] = mapped_column(DateTime)

    def __repr__(self) -> str:
        return f"""region_stats(
        type_id={self.type_id!r}, 
        total_volume_remain={self.total_volume_remain!r}, 
        min_price={self.min_price!r},
        price={self.price!r},
        avg_price={self.avg_price!r},
        avg_volume={self.avg_volume!r},
        group_id={self.group_id!r},
        type_name={self.type_name!r},   
        group_name={self.group_name!r},
        category_id={self.category_id!r},
        category_name={self.category_name!r},
        days_remaining={self.days_remaining!r},
        last_update={self.last_update!r}
        )"""

if __name__ == "__main__":
    pass

# Event listeners to automatically populate type_name fields
@event.listens_for(RegionHistory, 'before_insert')
def populate_region_history_type_name(mapper, connection, target):
    """Automatically populate type_name when inserting RegionHistory records"""
    if target.type_id and not target.type_name:
        try:
            target.type_name = get_type_name(target.type_id)
        except Exception as e:
            # If we can't get the type name, leave it as None
            pass


