import sqlalchemy as sa
from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import Session
from proj_config import wcmkt_url
import pandas as pd
from mkt_models import RegionHistory

doctrines = "nakah_doctrines.csv"
df = pd.read_csv(doctrines)
target_ids = df["typeid"].unique().tolist()

def get_history(type_ids):
    engine = create_engine(wcmkt_url)
    session = Session(bind=engine)
    stmt = select(RegionHistory).where(RegionHistory.type_id.in_(type_ids))
    result = session.execute(stmt)
    name = result.all()

if __name__ == "__main__":
    get_history(target_ids)
    
