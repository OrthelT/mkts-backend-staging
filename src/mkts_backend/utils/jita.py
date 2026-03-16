"""
Jita price utilities for fetching and working with Jita market prices.

Uses the Fuzzwork Market API for efficient bulk price lookups.
"""

import requests
from typing import Dict, List, Optional

from mkts_backend.config.logging_config import configure_logging

logger = configure_logging(__name__)

# Fuzzwork Market API endpoint for aggregated market data
FUZZWORK_API_URL = "https://market.fuzzwork.co.uk/aggregates/"

# The Forge region ID (Jita's region)
JITA_REGION_ID = 10000002


class JitaPrice:
    def __init__(self, type_id: int, price_data: dict):
        self.type_id = type_id
        self.buy_percentile = float(price_data['buy']['percentile'])
        self.buy_median = float(price_data['buy']['median'])
        self.buy_min = float(price_data['buy']['min'])
        self.sell_percentile = float(price_data['sell']['percentile'])
        self.sell_median = float(price_data['sell']['median'])
        self.sell_max = float(price_data['sell']['max'])
        self.sell_min = float(price_data['sell']['min'])
        self.sell_volume = float(price_data['sell']['volume'])
        self.buy_volume = float(price_data['buy']['volume'])
        self.buy_weightedAverage = float(price_data['buy']['weightedAverage'])

    def get_price_data(self) -> dict:
        return {
            'type_id': self.type_id,
            'sell_percentile': self.sell_percentile,
            'buy_percentile': self.buy_percentile
        }


def fetch_jita_prices(type_ids: List[int]) -> Dict[int, Optional[float]]:
    """
    Fetch Jita sell prices for a list of type IDs using Fuzzwork Market API.

    Uses the sell percentile (5th percentile of sell orders) as the reference price,
    which represents a reasonable buy price in Jita.

    Args:
        type_ids: List of type IDs to fetch prices for

    Returns:
        Dict mapping type_id to sell_percentile price (or None if not found)
    """
    if not type_ids:
        return {}

    results = {}

    # Fuzzwork API accepts comma-separated type IDs
    type_ids_str = ",".join(str(tid) for tid in type_ids)

    headers = {
        'User-Agent': 'wcmkts_backend/2.1, orthel.toralen@gmail.com',
        'Accept': 'application/json',
    }

    try:
        params = {
            'region': JITA_REGION_ID,
            'types': type_ids_str,
        }

        response = requests.get(FUZZWORK_API_URL, headers=headers, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        for type_id_str, price_data in data.items():
            type_id = int(type_id_str)
            try:
                # Use sell percentile (5th percentile) as the reference Jita price
                sell_percentile = float(price_data['sell']['percentile'])
                # Only use valid prices (non-zero)
                if sell_percentile > 0:
                    results[type_id] = sell_percentile
                else:
                    results[type_id] = None
            except (KeyError, ValueError, TypeError):
                results[type_id] = None

    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to fetch Jita prices: {e}")
        # Return None for all type_ids on failure
        for type_id in type_ids:
            results[type_id] = None

    # Fill in any missing type_ids with None
    for type_id in type_ids:
        if type_id not in results:
            results[type_id] = None

    return results


def fetch_jita_price_data(type_ids: List[int]) -> List[dict]:
    """Fetch full Jita price data (sell + buy) for DB storage.

    Uses Fuzzwork as primary source, Janice as fallback for failed items.

    Returns:
        List of dicts: [{"type_id": ..., "sell_price": ..., "buy_price": ..., "last_updated": ...}]
    """
    if not type_ids:
        return []

    from datetime import datetime, timezone
    import os

    now = datetime.now(timezone.utc).isoformat()
    results = {}
    failed_ids = []

    # --- Primary: Fuzzwork (batched to avoid 414 URI Too Large) ---
    BATCH_SIZE = 250
    headers = {
        'User-Agent': 'wcmkts_backend/2.1, orthel.toralen@gmail.com',
        'Accept': 'application/json',
    }
    chunks = [type_ids[i:i + BATCH_SIZE] for i in range(0, len(type_ids), BATCH_SIZE)]

    for chunk_idx, chunk in enumerate(chunks):
        type_ids_str = ",".join(str(tid) for tid in chunk)
        try:
            params = {
                'region': JITA_REGION_ID,
                'types': type_ids_str,
            }
            response = requests.get(FUZZWORK_API_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fuzzwork chunk {chunk_idx + 1}/{len(chunks)}: {len(data)} items")

            for type_id in chunk:
                type_id_str = str(type_id)
                if type_id_str not in data:
                    failed_ids.append(type_id)
                    continue
                try:
                    price_data = data[type_id_str]
                    sell_price = float(price_data['sell']['percentile'])
                    buy_price = float(price_data['buy']['percentile'])
                    if sell_price > 0 or buy_price > 0:
                        results[type_id] = {
                            "type_id": type_id,
                            "sell_price": sell_price,
                            "buy_price": buy_price,
                            "last_updated": now,
                        }
                    else:
                        failed_ids.append(type_id)
                except (KeyError, ValueError, TypeError):
                    failed_ids.append(type_id)

        except requests.exceptions.RequestException as e:
            logger.warning(f"Fuzzwork chunk {chunk_idx + 1}/{len(chunks)} failed: {e}")
            failed_ids.extend(chunk)

    # --- Fallback: Janice for failed items ---
    janice_key = os.environ.get("JANICE_KEY")
    if failed_ids and janice_key:
        logger.info(f"Trying Janice fallback for {len(failed_ids)} items")
        janice_url = "https://janice.e-351.com/api/rest/v2/pricer"
        janice_headers = {
            'X-ApiKey': janice_key,
            'accept': 'application/json',
            'Content-Type': 'text/plain',
        }
        try:
            body = '\n'.join(str(tid) for tid in failed_ids)
            resp = requests.post(
                janice_url,
                data=body,
                headers=janice_headers,
                params={'market': '2'},
                timeout=30,
            )
            resp.raise_for_status()
            janice_data = resp.json()

            for item in janice_data:
                type_id = item.get('typeID')
                if type_id is None:
                    continue
                prices = item.get('top5AveragePrices', {})
                sell_price = float(prices.get('sellPrice', 0) or 0)
                buy_price = float(prices.get('buyPrice', 0) or 0)
                if sell_price > 0 or buy_price > 0:
                    results[type_id] = {
                        "type_id": type_id,
                        "sell_price": sell_price,
                        "buy_price": buy_price,
                        "last_updated": now,
                    }
        except requests.exceptions.RequestException as e:
            logger.warning(f"Janice API failed: {e}")
    elif failed_ids and not janice_key:
        logger.info(f"No JANICE_KEY set, skipping Janice fallback for {len(failed_ids)} items")

    logger.info(f"Jita price data: {len(results)} items fetched ({len(type_ids)} requested)")
    return list(results.values())


def get_overpriced_items(
    market_data: List[Dict],
    threshold: float = 1.2,
) -> List[Dict]:
    """
    Get items whose local market price exceeds the Jita price by a threshold.

    Args:
        market_data: List of item dicts with 'price' and 'jita_price' keys
        threshold: Price ratio threshold (1.2 = 120% of Jita price)

    Returns:
        List of overpriced items with price comparison data
    """
    overpriced = []

    for item in market_data:
        local_price = item.get("price")
        jita_price = item.get("jita_price")

        if local_price and jita_price and jita_price > 0:
            price_ratio = local_price / jita_price
            if price_ratio > threshold:
                overpriced.append({
                    "type_id": item.get("type_id"),
                    "type_name": item.get("type_name"),
                    "local_price": local_price,
                    "jita_price": jita_price,
                    "price_ratio": price_ratio,
                    "percent_above_jita": (price_ratio - 1) * 100,
                })

    # Sort by price ratio (highest first)
    overpriced.sort(key=lambda x: x["price_ratio"], reverse=True)

    return overpriced


if __name__ == "__main__":
    pass
