from sqlalchemy import text
import pandas as pd
from mkts_backend.config.config import DatabaseConfig

def get_market_history(type_id: int) -> pd.DataFrame:
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        stmt = "SELECT * FROM market_history WHERE type_id = ?"
        result = conn.execute(stmt, (type_id,))
        headers = [col[0] for col in result.description]
    conn.close()
    return pd.DataFrame(result.fetchall(), columns=headers)

def get_market_orders(type_id: int) -> pd.DataFrame:
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        stmt = "SELECT * FROM market_orders WHERE type_id = ?"
        result = conn.execute(stmt, (type_id,))
        headers = [col[0] for col in result.description]
    conn.close()
    return pd.DataFrame(result.fetchall(), columns=headers)

def get_market_stats(type_id: int) -> pd.DataFrame:
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM marketstats WHERE type_id = :type_id")
        df = pd.read_sql_query(stmt, conn, params={"type_id": type_id})
    conn.close()
    return df

def get_doctrine_stats(type_id: int) -> pd.DataFrame:
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM doctrines WHERE type_id = :type_id")
        df = pd.read_sql_query(stmt, conn, params={"type_id": type_id})
    conn.close()
    return df

def get_table_length(table: str) -> int:
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        stmt = text(f"SELECT COUNT(*) FROM {table}")
        result = conn.execute(stmt)
        return result.fetchone()[0]

def get_watchlist_ids():
    stmt = text("SELECT DISTINCT type_id FROM watchlist")
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        result = conn.execute(stmt)
        watchlist_ids = [row[0] for row in result]
    conn.close()
    engine.dispose()
    return watchlist_ids

def get_fit_items(fit_id: int) -> list[int]:
    stmt = text("SELECT type_id FROM fittings_fittingitem WHERE fit_id = :fit_id")
    db = DatabaseConfig("fittings")
    engine = db.engine
    with engine.connect() as conn:
        result = conn.execute(stmt, {"fit_id": fit_id})
        fit_items = [row[0] for row in result]
    conn.close()
    engine.dispose()
    return fit_items

def get_fit_ids(doctrine_id: int):
    stmt = text("SELECT fitting_id FROM fittings_doctrine_fittings WHERE doctrine_id = :doctrine_id")
    db = DatabaseConfig("fittings")
    engine = db.engine
    with engine.connect() as conn:
        result = conn.execute(stmt, {"doctrine_id": doctrine_id})
        fit_ids = [row[0] for row in result]
    conn.close()
    engine.dispose()
    return fit_ids

if __name__ == "__main__":
    pass
