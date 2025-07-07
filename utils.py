import pandas as pd
from logging_config import configure_logging
import time
import json
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy import text
from proj_config import wcmkt_url
import requests

logger = configure_logging(__name__)

def get_type_names(df: pd.DataFrame) -> pd.DataFrame:
    engine = sa.create_engine(wcmkt_url)
    with engine.connect() as conn:
        stmt = text("SELECT type_id, type_name FROM watchlist")
        res = conn.execute(stmt)
        df = pd.DataFrame(res.fetchall(), columns=["type_id", "type_name"])
    engine.dispose()
    return df[["type_id", "type_name"]] 

def get_type_name_from_esi(ids: list[int]) -> pd.DataFrame:
    url = "https://esi.evetech.net/latest/universe/names/?datasource=tranquility"
    headers = {
        "Accept": "application/json",
        "User-Agent": "WC Mkts Dev, orthel.toralen@gmail.com",
    }
    response = requests.post(url, json={"ids": ids}, headers=headers)
    data = response.json()
    df = pd.DataFrame(data)
    df = df[["id", "name"]]
    df = df.rename(columns={"id": "type_id", "name": "type_name"})
    return df


def get_null_count(df):
    return df.isnull().sum()


def validate_columns(df, valid_columns):
    return df[valid_columns]


def validate_type_names(df):
    return df[df["type_name"].notna()]


def validate_type_ids(df):
    return df[df["type_id"].notna()]


def validate_order_ids(df):
    return df[df["order_id"].notna()]


def add_timestamp(df):
    df["timestamp"] = pd.Timestamp.now(tz="UTC")
    df["timestamp"] = df["timestamp"].dt.to_pydatetime()
    return df


def add_autoincrement(df):
    df["id"] = df.index + 1
    return df


def standby(seconds: int):
    for i in range(seconds):
        message = f"\rWaiting for {seconds - i} seconds"
        print(message, end="", flush=True)
        time.sleep(1)
    print()


def simulate_market_orders() -> dict:
    with open("market_orders.json", "r") as f:
        data = json.load(f)
    return data


def simulate_market_history() -> dict:
    df = pd.read_csv("data/valemarkethistory_2025-05-13_08-06-00.csv")
    watchlist = pd.read_csv("data/all_watchlist.csv")
    watchlist = watchlist[["type_id", "type_name"]]
    df = df.merge(watchlist, on="type_id", how="left")
    df = df[
        [
            "average",
            "date",
            "highest",
            "lowest",
            "order_count",
            "volume",
            "type_name",
            "type_id",
        ]
    ]
    return df.to_dict(orient="records")


def get_status():
    engine = sa.create_engine(wcmkt_url)
    with engine.connect() as conn:
        dcount = conn.execute(text("SELECT COUNT(id) FROM doctrines"))
        doctrine_count = dcount.fetchone()[0]
        order_count = conn.execute(text("SELECT COUNT(order_id) FROM marketorders"))
        order_count = order_count.fetchone()[0]
        history_count = conn.execute(text("SELECT COUNT(id) FROM market_history"))
        history_count = history_count.fetchone()[0]
        stats_count = conn.execute(text("SELECT COUNT(type_id) FROM marketstats"))
        stats_count = stats_count.fetchone()[0]
        region_orders_count = conn.execute(text("SELECT COUNT(order_id) FROM region_orders"))
        region_orders_count = region_orders_count.fetchone()[0]
    engine.dispose()
    print(f"Doctrines: {doctrine_count}")
    print(f"Market Orders: {order_count}")
    print(f"Market History: {history_count}")
    print(f"Market Stats: {stats_count}")
    print(f"Region Orders: {region_orders_count}")
    status_dict = {
        "doctrines": doctrine_count,
        "market_orders": order_count,
        "market_history": history_count,
        "market_stats": stats_count,
        "region_orders": region_orders_count,
    }



    # timestamp = time.time()
    # with open(f"status_{timestamp}.json", "w") as f:
    #     json.dump(status_dict, f)

def sleep_for_seconds(seconds: int):
    for i in range(seconds):
        message = f"\rWaiting for {seconds - i} seconds"
        print(message, end="", flush=True)
        time.sleep(1)
    print()


if __name__ == "__main__":
     ids = [183, 184, 185, 186, 187, 189, 190, 197, 215, 218, 220, 221, 222, 224, 225, 227, 228, 230, 231, 233, 235, 236, 237, 238, 242, 246, 247, 250, 254, 380, 434, 438, 439, 440, 444, 447, 448, 450, 484, 499, 519, 527, 578, 582, 585, 599, 602, 632, 644, 672, 1182, 1201, 1236, 1266, 1306, 1319, 1355, 1405, 1422, 1447, 1539, 1541, 1565, 1855, 1875, 1877, 1952, 1964, 1969, 1978, 1987, 1999, 2024, 2046, 2048, 2109, 2161, 2173, 2175, 2185, 2193, 2203, 2281, 2293, 2299, 2301, 2333, 2364, 2404, 2410, 2420, 2436, 2446, 2464, 2476, 2486, 2488, 2512, 2516, 2553, 2559, 2563, 2567, 2571, 2575, 2603, 2605, 2629, 2873, 2881, 2889, 2897, 2913, 2929, 2961, 2969, 2977, 2993, 3001, 3025, 3033, 3041, 3057, 3074, 3082, 3098, 3162, 3170, 3178, 3186, 3240, 3244, 3520, 3554, 3568, 3608, 3651, 3687, 3829, 3831, 3839, 3841, 3888, 3898, 3947, 3955, 4025, 4027, 4254, 4258, 4313, 4314, 4315, 4316, 4471, 4477, 4871, 5093, 5137, 5141, 5299, 5302, 5319, 5320, 5322, 5339, 5342, 5399, 5403, 5405, 5439, 5443, 5445, 5631, 5839, 5933, 5945, 5971, 5973, 5975, 6001, 6003, 6160, 6176, 6296, 6569, 7249, 7579, 7583, 7743, 7745, 8027, 8089, 8105, 8175, 8263, 8419, 8433, 8517, 8521, 8529, 8535, 8537, 8579, 8583, 8635, 8639, 8641, 8787, 9133, 9253, 9369, 9417, 9521, 9580, 9622, 9632, 9944, 10190, 10228, 10631, 10680, 10876, 10998, 11105, 11133, 11174, 11178, 11182, 11190, 11194, 11196, 11198, 11269, 11289, 11325, 11370, 11379, 11387, 11400, 11563, 11577, 11578, 11640, 11859, 11870, 11872, 11873, 11883, 11887, 11889, 11890, 11891, 11961, 11963, 11971, 11978, 11985, 11987, 11995, 12013, 12023, 12032, 12034, 12044, 12058, 12068, 12076, 12084, 12102, 12199, 12259, 12267, 12297, 12300, 12301, 12557, 12559, 12608, 12612, 12614, 12618, 12620, 12625, 12631, 12633, 12765, 12767, 12771, 12773, 12777, 12801, 12805, 12814, 12816, 12818, 12820, 12822, 12826, 13001, 13003, 13209, 13226, 13233, 13244, 13923, 13954, 13976, 14047, 14104, 14130, 14134, 14244, 14262, 15463, 15464, 15729, 15766, 16274, 16275, 16391, 16447, 16451, 16467, 16469, 16475, 16477, 16487, 16489, 16495, 16497, 16499, 16505, 16507, 16519, 16521, 16525, 16527, 17322, 17324, 17325, 17326, 17333, 17342, 17346, 17347, 17355, 17713, 17841, 17887, 17888, 17889, 17938, 18635, 18637, 18639, 18803, 18841, 19141, 19147, 19270, 19271, 19281, 19282, 19301, 19325, 19660, 19739, 19806, 19812, 19814, 19923, 19927, 19929, 19939, 19942, 19944, 19946, 19948, 19952, 19962, 20739, 20741, 20743, 20963, 21640, 21740, 21857, 21889, 21894, 21896, 21898, 21902, 21904, 21906, 21910, 21912, 21914, 21922, 21924, 21926, 21928, 21935, 21937, 21939, 22291, 22452, 22456, 22464, 22468, 22542, 22576, 22778, 22782, 22961, 22963, 22973, 22977, 23009, 23023, 23025, 23029, 23031, 23035, 23039, 23071, 23089, 23527, 23563, 23707, 23709, 23711, 23719, 23727, 24417, 24427, 24507, 24509, 24511, 24519, 24523, 24698, 24702, 25267, 25544, 25563, 25565, 25715, 25861, 25894, 25929, 25931, 25933, 25935, 25937, 25949, 25951, 25953, 25955, 25957, 26026, 26039, 26041, 26043, 26047, 26049, 26057, 26059, 26061, 26063, 26067, 26069, 26071, 26073, 26088, 26442, 26888, 26892, 26914, 27127, 27321, 27333, 27339, 27345, 27351, 27361, 27435, 27441, 27447, 27453, 27914, 27916, 27918, 27920, 27924, 28207, 28211, 28213, 28302, 28326, 28332, 28334, 28336, 28576, 28578, 28646, 28668, 28672, 28674, 28678, 28680, 28756, 28758, 28999, 29000, 29001, 29002, 29003, 29004, 29005, 29006, 29007, 29008, 29009, 29010, 29011, 29012, 29013, 29014, 29015, 29016, 29336, 29340, 29344, 30013, 30486, 30488, 30993, 31047, 31055, 31106, 31108, 31110, 31117, 31118, 31120, 31121, 31122, 31130, 31132, 31134, 31142, 31144, 31146, 31154, 31155, 31156, 31158, 31159, 31160, 31162, 31164, 31165, 31167, 31173, 31178, 31180, 31182, 31183, 31190, 31192, 31194, 31202, 31204, 31206, 31214, 31216, 31218, 31227, 31229, 31231, 31239, 31241, 31243, 31251, 31253, 31255, 31274, 31276, 31286, 31298, 31342, 31359, 31360, 31361, 31366, 31371, 31373, 31383, 31385, 31395, 31397, 31407, 31409, 31604, 31606, 31616, 31657, 31659, 31661, 31669, 31671, 31673, 31681, 31683, 31685, 31693, 31695, 31697, 31705, 31707, 31709, 31716, 31718, 31722, 31724, 31746, 31748, 31760, 31788, 31790, 31794, 31796, 31880, 31922, 31926, 31928, 31932, 31952, 31982, 31990, 31998, 32006, 32014, 32414, 33157, 33177, 33179, 33181, 33272, 33332, 33474, 33517, 33518, 33590, 33592, 34260, 34267, 34269, 34307, 34309, 34562, 34593, 34595, 35656, 35657, 35658, 35659, 35660, 35661, 35662, 35771, 35789, 35790, 35794, 35795, 35796, 35797, 37457, 37458, 37460, 37479, 37481, 37482, 37544, 37546, 37608, 37610, 37611, 37843, 37844, 40334, 40335, 40336, 40337, 41062, 41155, 41218, 41230, 41324, 41464, 41482, 41489, 41490, 41517, 41518, 41590, 41591, 41592, 41593, 41594, 41595, 41596, 41597, 42529, 42694, 42695, 42696, 42835, 42838, 42839, 42840, 43552, 43554, 43555, 43556, 43894, 43902, 43904, 43906, 43908, 44102, 45010, 45011, 45590, 45595, 45596, 45607, 45608, 45621, 45626, 45632, 45633, 45998, 45999, 46004, 46005, 46369, 47257, 47887, 47911, 47914, 47925, 47926, 47932, 47933, 48114, 49710, 52694, 52915, 52916, 53301, 53302, 54291, 56748, 57429, 58905, 58966, 58972, 59632, 60297, 62450, 62451, 62452, 62453, 72811, 73794, 76963, 77402]

     df = get_type_name_from_esi(ids)
     print(df)
