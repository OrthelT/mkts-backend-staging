import pandas as pd
from sqlalchemy import create_engine, select, text
from datetime import datetime, timedelta
from proj_config import wcmkt_db_path, wcmkt_local_url, sde_local_path, sde_local_url
from models import *
from dbhandler import get_market_history, get_wcmkt_local_engine, get_region_deployment_history, get_wcmkt_remote_engine, sde_local_engine
from millify import millify

def aggregate_region_history():
    """
    Aggregate region history by type_id and calculate total value.
    """
    engine = get_wcmkt_local_engine()
    with engine.connect() as conn:
        stmt = text("SELECT * FROM region_history")
        result = conn.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    df = df.sort_values(by="date", ascending=False)

    df2 = df.copy()
    df2 = df2.reset_index(drop=True)

    df2["date"] = pd.to_datetime(df2["date"])
    df2 = df2.sort_values(by="date", ascending=True)
    df2 = df2[df2.date > "2025-07-05 00:00:00"]
    df2 = df2.reset_index(drop=True)

    df3 = df2.copy()
    df3 = df3.groupby("type_id").agg({"average": "mean", "volume": "sum"}).reset_index()
    df3["total_value"] = df3["average"] * df3["volume"]
    df3 = df3.sort_values(by="total_value", ascending=False)

    engine = create_engine(sde_local_url)
    with engine.connect() as conn:
        stmt = text("SELECT * FROM inv_info")
        result = conn.execute(stmt)
        df4 = pd.DataFrame(result.fetchall(), columns=result.keys())
    df4 = df4.sort_values(by="typeName", ascending=True)
    df4 = df4.reset_index(drop=True)
    df4 = df4[["typeID", "typeName", "groupID", "groupName", "categoryID", "categoryName"]]

    df5 = df3.copy()
    df5 = df5.merge(df4, left_on="type_id", right_on="typeID", how="left")
    df5 = df5[["typeID", "typeName", "groupID", "groupName", "categoryID", "categoryName", "average", "volume", "total_value"]]
    df5 = df5.sort_values(by="total_value", ascending=False)

    return df5

def aggregate_region_history_by_category():
    """
    Aggregate region history by category and calculate total value.
    """
    engine = get_wcmkt_local_engine()
    with engine.connect() as conn:
        stmt = text("SELECT * FROM region_history")
        result = conn.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    df = df.sort_values(by="date", ascending=False)
    df2 = df.copy()
    df2 = df2.reset_index(drop=True)
    df2["date"] = pd.to_datetime(df2["date"])
    df2 = df2.sort_values(by="date", ascending=True)
    df2 = df2[df2.date > "2025-07-05 00:00:00"]
    df2 = df2.reset_index(drop=True)
    df2 = df2.groupby("type_id").agg({"average": "mean", "volume": "sum"}).reset_index()
    df2["total_value"] = df2["average"] * df2["volume"]
    df2 = df2.sort_values(by="total_value", ascending=False)
    df2 = df2.reset_index(drop=True)
    engine = create_engine(sde_local_url)
    with engine.connect() as conn:
        stmt = text("SELECT * FROM inv_info")
        result = conn.execute(stmt)
        df4 = pd.DataFrame(result.fetchall(), columns=result.keys())
    df4 = df4.sort_values(by="typeName", ascending=True)
    df4 = df4.reset_index(drop=True)
    df4 = df4[["typeID", "typeName", "groupID", "groupName", "categoryID", "categoryName"]]
    df2 = df2.merge(df4, left_on="type_id", right_on="typeID", how="left")
    df2 = df2[["typeID", "typeName", "groupID", "groupName", "categoryID", "categoryName", "average", "volume", "total_value"]]
    df2 = df2.groupby("categoryName").agg({"total_value": "sum", "volume": "sum"}).reset_index()
    df2["total_value"] = df2["total_value"].apply(lambda x: millify(x, precision=2))
    df2["volume"] = df2["volume"].apply(lambda x: millify(x, precision=2))
    df2 = df2.sort_values(by="total_value", ascending=False)
    df2 = df2.reset_index(drop=True)
    return df2

def aggregate_region_history_by_group():
    """
    Aggregate region history by group and calculate total value.
    """
    engine = get_wcmkt_local_engine()
    with engine.connect() as conn:
        stmt = text("SELECT * FROM region_history")
        result = conn.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    df = df.sort_values(by="date", ascending=False)
    df2 = df.copy()
    df2 = df2.reset_index(drop=True)
    df2["date"] = pd.to_datetime(df2["date"])
    df2 = df2.sort_values(by="date", ascending=True)
    df2 = df2[df2.date > "2025-07-05 00:00:00"]
    df2 = df2.reset_index(drop=True)
    df2 = df2.groupby("type_id").agg({"average": "mean", "volume": "sum"}).reset_index()
    df2["total_value"] = df2["average"] * df2["volume"]
    df2 = df2.sort_values(by="total_value", ascending=False)
    df2 = df2.reset_index(drop=True)
    engine = create_engine(sde_local_url)
    with engine.connect() as conn:
        stmt = text("SELECT * FROM inv_info")
        result = conn.execute(stmt)
        df4 = pd.DataFrame(result.fetchall(), columns=result.keys())
    df4 = df4.sort_values(by="typeName", ascending=True)
    df4 = df4.reset_index(drop=True)
    df4 = df4[["typeID", "typeName", "groupID", "groupName", "categoryID", "categoryName"]]
    df2 = df2.merge(df4, left_on="type_id", right_on="typeID", how="left")
    df2 = df2[["typeID", "typeName", "groupID", "groupName", "categoryID", "categoryName", "average", "volume", "total_value"]]
    df2 = df2.groupby("groupName").agg({"total_value": "sum", "volume": "sum"}).reset_index()
    df2["total_value"] = df2["total_value"].apply(lambda x: millify(x, precision=2))
    df2["volume"] = df2["volume"].apply(lambda x: millify(x, precision=2))
    df2 = df2.sort_values(by="total_value", ascending=False)
    df2 = df2.reset_index(drop=True)
    return df2

def aggregate_region_history_by_type():
    """
    Aggregate region history by type and calculate total value.
    """
    engine = get_wcmkt_local_engine()
    with engine.connect() as conn:
        stmt = text("SELECT * FROM region_history")
        result = conn.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    df = df.sort_values(by="date", ascending=False)
    df2 = df.copy()
    df2 = df2.reset_index(drop=True)
    df2["date"] = pd.to_datetime(df2["date"])
    df2 = df2.sort_values(by="date", ascending=True)
    df2 = df2[df2.date > "2025-07-05 00:00:00"]
    df2 = df2.reset_index(drop=True)
    df2 = df2.groupby("type_id").agg({"average": "mean", "volume": "sum"}).reset_index()
    df2["total_value"] = df2["average"] * df2["volume"]
    df2 = df2.sort_values(by="total_value", ascending=False)
    df2 = df2.reset_index(drop=True)
    engine = create_engine(sde_local_url)
    with engine.connect() as conn:
        stmt = text("SELECT * FROM inv_info")
        result = conn.execute(stmt)
        df4 = pd.DataFrame(result.fetchall(), columns=result.keys())
    df4 = df4.sort_values(by="typeName", ascending=True)
    df4 = df4.reset_index(drop=True)
    df4 = df4[["typeID", "typeName", "groupID", "groupName", "categoryID", "categoryName"]]
    df2 = df2.merge(df4, left_on="type_id", right_on="typeID", how="left")
    df2 = df2[["typeID", "typeName", "groupID", "groupName", "categoryID", "categoryName", "average", "volume", "total_value"]]
    df2 = df2.sort_values(by="total_value", ascending=False)
    df2 = df2.reset_index(drop=True)
    df2["total_value"] = df2["total_value"].apply(lambda x: millify(x, precision=2))
    df2["volume"] = df2["volume"].apply(lambda x: millify(x, precision=2))
    df2["average"] = df2["average"].apply(lambda x: millify(x, precision=2))
    return df2
def aggregate_region_history_by_ship():
    """
    Aggregate region history by ship and calculate total value.
    """
    engine = get_wcmkt_local_engine()
    with engine.connect() as conn:
        stmt = text("SELECT * FROM region_history")
        result = conn.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    df = df.sort_values(by="date", ascending=False)
    df2 = df.copy()
    df2 = df2.reset_index(drop=True)
    df2["date"] = pd.to_datetime(df2["date"])
    df2 = df2.sort_values(by="date", ascending=True)
    df2 = df2[df2.date > "2025-07-05 00:00:00"]
    df2 = df2.reset_index(drop=True)
    df2 = df2.groupby("type_id").agg({"average": "mean", "volume": "sum"}).reset_index()
    df2["total_value"] = df2["average"] * df2["volume"]
    df2 = df2.sort_values(by="total_value", ascending=False)
    df2 = df2.reset_index(drop=True)
    engine = create_engine(sde_local_url)
    with engine.connect() as conn:
        stmt = text("SELECT * FROM inv_info")
        result = conn.execute(stmt)
        df4 = pd.DataFrame(result.fetchall(), columns=result.keys())
    df4 = df4.sort_values(by="typeName", ascending=True)
    df4 = df4.reset_index(drop=True)
    df4 = df4[["typeID", "typeName", "groupID", "groupName", "categoryID", "categoryName"]]
    df2 = df2.merge(df4, left_on="type_id", right_on="typeID", how="left")
    df2 = df2[["typeID", "typeName", "groupID", "groupName", "categoryID", "categoryName", "average", "volume", "total_value"]]
    df2 = df2[df2["categoryName"] == "Ship"]
    df2 = df2.sort_values(by="total_value", ascending=False)
    df2 = df2.reset_index(drop=True)
    df2["total_value"] = df2["total_value"].apply(lambda x: millify(x, precision=2))
    df2["volume"] = df2["volume"].apply(lambda x: millify(x, precision=2))
    df2["average"] = df2["average"].apply(lambda x: millify(x, precision=2))
    return df2

if __name__ == "__main__":
    df2 = aggregate_region_history_by_ship()
    print(df2)

    df3 = aggregate_region_history()
    total_value = df3["total_value"].sum()
    print(millify(total_value, precision=2))
