from os import sync
from mkts_backend.config.config import DatabaseConfig
from sqlalchemy import text, insert, select, MetaData, inspect
from sqlalchemy.orm import Session
from mkts_backend.db.models import Doctrines, Base
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.processing.data_processing import calculate_doctrine_stats

logger = configure_logging(__name__)

mkt_db = DatabaseConfig("wcmkt")
fits_db = DatabaseConfig("fittings")
sde_db = DatabaseConfig("sde")

sde_engine = sde_db.remote_engine
fits_engine = fits_db.remote_engine
mkt_engine = mkt_db.remote_engine

def get_fit_items(fit_id: int, ship_id: int, ship_name: str)->list[Doctrines]:
    stmt = text("SELECT * FROM fittings_fittingitem WHERE fit_id = 494")
    items = []
    with fits_engine.connect() as conn:
        result = conn.execute(stmt)
        rows = result.fetchall()
        for row in rows:
            item = Doctrines(
                fit_id=fit_id,
                ship_id=ship_id,
                type_id=row.type_id,
                ship_name=ship_name,
                fit_qty=row.quantity,
            )
            items.append(item)
    return items

def update_items(items: list[Doctrines]):
    updated_items = []
    with sde_engine.connect() as conn:
        for item in items:
            result = conn.execute(text("SELECT * FROM inv_info WHERE typeID = :type_id"), {"type_id": item.type_id})
            new_item = result.fetchone()
            item.type_name = new_item.typeName
            item.group_name = new_item.groupName
            item.category_name = new_item.categoryName
            item.category_id = new_item.categoryID
            item.group_id = new_item.groupID
            updated_items.append(item)
    return updated_items

def add_items_to_doctrines_table(items: list[Doctrines]):
    engine = mkt_engine
    session = Session(engine)
    with session.begin():
        try:
            for item in items:
                session.add(item)
                logger.info(f"Added {item.type_name} to doctrines {item.fit_id}")
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error adding items to doctrines table: {e}")
            raise
        finally:
            session.close()
            engine.dispose()

def add_fit_to_doctrines_table(fit_id: int, ship_id: int, ship_name: str):
    items = get_fit_items(fit_id, ship_id, ship_name)
    updated_items = update_items(items)
    add_items_to_doctrines_table(updated_items)

def select_doctrines_table(fit_id: int)->list[dict]:
    engine = mkt_engine
    session = Session(engine)
    items = []

    with session.begin():
        result = select(Doctrines).where(Doctrines.fit_id == fit_id)
        for item in session.scalars(result):

            item = item.__dict__
            item.pop('_sa_instance_state')

            items.append(item)
    session.close()
    engine.dispose()
    return items

if __name__ == "__main__":
    pass