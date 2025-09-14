import pandas as pd
from sqlalchemy import text, insert
from sqlalchemy.orm import session
from mkts_backend.config.config import DatabaseConfig
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.db.models import Watchlist
from mkts_backend.utils import get_type_info
from mkts_backend.utils.utils import init_databases

logger = configure_logging(__name__)

sde_db = DatabaseConfig("sde")
wcmkt_db = DatabaseConfig("wcmkt")

def add_missing_items_to_watchlist(missing_items: list[int]):
    engine = sde_db.engine
    with engine.connect() as conn:
        from sqlalchemy import bindparam
        stmt = text("SELECT * FROM inv_info WHERE typeID IN :missing").bindparams(bindparam('missing', expanding=True))
        res = conn.execute(stmt, {"missing": missing_items})
        df = pd.DataFrame(res.fetchall())
        df.columns = res.keys()
        df = df.rename(columns={"typeID": "type_id", "typeName": "type_name", "groupID": "group_id", "groupName": "group_name", "categoryID": "category_id", "categoryName": "category_name"})

    watchlist = wcmkt_db.get_watchlist()
    inv_cols = ['type_id', 'type_name', 'group_id', 'group_name', 'category_id', 'category_name']
    df = df[inv_cols]
    watchlist_cols = ['type_id', 'type_name', 'group_id', 'group_name', 'category_id', 'category_name']
    watchlist = pd.concat([watchlist, df], ignore_index=True)
    watchlist.to_csv("data/watchlist_updated.csv", index=False)
    watchlist.to_sql("watchlist", wcmkt_db.engine, if_exists="append", index=False)

def update_watchlist_tables(missing_items: list[int]):
    engine = sde_db.engine
    with engine.connect() as conn:
        from sqlalchemy import bindparam
        stmt = text("SELECT * FROM inv_info WHERE typeID IN :missing").bindparams(bindparam('missing', expanding=True))
        res = conn.execute(stmt, {"missing": missing_items})
        df = pd.DataFrame(res.fetchall())
        df.columns = res.keys()

    inv_cols = ['typeID', 'typeName', 'groupID', 'groupName', 'categoryID', 'categoryName']
    watchlist_cols = ['type_id', 'type_name', 'group_id', 'group_name', 'category_id', 'category_name']
    df = df[inv_cols]
    df = df.rename(columns=dict(zip(inv_cols, watchlist_cols)))

    engine = wcmkt_db.engine
    with engine.connect() as conn:
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


if __name__ == "__main__":
    pass