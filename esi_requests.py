from config import ESIConfig, DatabaseConfig
from logging_config import configure_logging
import requests
import time
import json
from requests.exceptions import ReadTimeout
from ESI_OAUTH_FLOW import get_token
import pandas as pd

logger = configure_logging(__name__)

def fetch_market_orders(esi: ESIConfig, order_type: str = "all", etag: str = None) -> list[dict]:
    logger.info("Fetching market orders")
    """
    order_type: str = "all" | "buy" | "sell" (default is "all", only used for secondary market)
    page: int = 1 is the default page number. This can be used to fetch a single page of orders, or as an argument dynamically updated in a loop.
    etag: str = None is the etag of the last response for the requested page. This is used to optionally check for changes in the market orders. The esi will return a 304 if the etag is the same as the last response.

    Returns:
        requests.Response: Response object containing the market orders
    Raises:
        ValueError: If the alias is invalid
    """
    page = 1
    max_pages = 1
    orders = []
    error_count = 0
    request_count = 0


    url = esi.market_orders_url
    headers = esi.headers


    while page <= max_pages:
        request_count += 1

        logger.info(f"NEW REQUEST: request_count: {request_count}, page: {page}, max_pages: {max_pages}")

        if esi.alias == "primary":
            querystring = {"page": str(page)}
            logger.info(f"querystring: {querystring}")
        elif esi.alias == "secondary":
            querystring = {"page": str(page), "order_type": order_type}
        else:
            raise ValueError(f"Invalid alias: {esi.alias}. Valid aliases are: {esi._valid_aliases}")

        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()

        logger.info(f"response: {response.status_code}")

        if response.status_code == 200:
            logger.info(f"response successful: {response.status_code}")
            data = response.json()
            orders.extend(data)
            page += 1
            max_pages = int(response.headers.get("X-Pages"))
            logger.info(f"response headers: {response.headers}")

            logger.info(f"page: {page}, max_pages: {max_pages} ...sleeping for 0.5 seconds")
            time.sleep(0.5)

        else:
            logger.error(f"Error fetching market orders: {response.status_code}")
            error_count += 1
            if error_count > 3:
                logger.error("Too many errors, stopping")
                return None
            else:
                logger.error(f"Retrying... {error_count} attempts")
                time.sleep(5)

    logger.info(f"market_orders complete: {len(orders)} orders")


    return orders


def fetch_history(watchlist: pd.DataFrame) -> list[dict]:
    esi = ESIConfig("primary")
    url = esi.market_history_url
    error_count = 0
    total_time_taken = 0

    logger.info("Fetching history")
    if watchlist is None or watchlist.empty:
        logger.error("No watchlist provided or watchlist is empty")
        return None
    else:
        logger.info("Watchlist found")
        print(f"Watchlist found: {len(watchlist)} items")

    type_ids = watchlist["type_id"].tolist()
    logger.info(f"Fetching history for {len(type_ids)} types")

    headers = esi.headers()
    del headers["Authorization"]

    history = []
    request_count = 0

    watchlist_length = len(type_ids)

    while request_count < watchlist_length:
        type_id = type_ids[request_count]
        item_name = watchlist[watchlist["type_id"] == type_id]["type_name"].values[0]
        logger.info(f"Fetching history for {item_name}: {type_id}")
        querystring = {"type_id": type_id}
        request_count += 1
        try:

            print(
                f"\rFetching history for ({request_count}/{watchlist_length})",
                end="",
                flush=True,
            )
            t1 = time.perf_counter()
            response = requests.get(url, headers=headers, timeout=10, params=querystring)
            response.raise_for_status()

            if response.status_code == 200:
                logger.info(f"response successful: {response.status_code}")
                error_remain = int(response.headers.get("X-Esi-Error-Limit-Remain"))
                if error_remain < 100:
                    logger.info(f"error_remain: {error_remain}")

                data = response.json()
                for record in data:
                    record["type_name"] = item_name
                    record["type_id"] = type_id

                if isinstance(data, list):
                    history.extend(data)
                else:
                    logger.warning(f"Unexpected data format for {item_name}")
            else:
                logger.error(
                    f"Error fetching history for {item_name}: {response.status_code}"
                )

        except Exception as e:
            logger.error(f"Error processing {item_name}: {e}")
            error_count += 1
            if error_count > 10:
                logger.error(f"Too many errors, stopping. Error count: {error_count}")
                return None
            else:
                logger.error(f"Retrying... {error_count} attempts")
                time.sleep(3)
            continue
        t2 = time.perf_counter()
        time_taken = round(t2 - t1, 2)
        total_time_taken += time_taken
        logger.info(f"time: {time_taken}s, average: {round(total_time_taken / request_count, 2)}s")
        if time_taken < 0.25:
            time.sleep(0.5)
            print(f"sleeping for 0.5 seconds to avoid rate limiting. Time: {time_taken}s")
    if history:
        logger.info(f"Successfully fetched {len(history)} total history records")
        with open("data/market_history.json", "w") as f:
            json.dump(history, f)
        return history
    else:
        logger.error("No history records found")
        return None


if __name__ == "__main__":
    pass