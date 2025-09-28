import asyncio
import random
import time
import httpx
from aiolimiter import AsyncLimiter
import backoff
from datetime import datetime
from mkts_backend.config.config import DatabaseConfig
from mkts_backend.config.esi_config import ESIConfig
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.db.models import JitaHistory
from mkts_backend.utils.utils import get_type_name

logger = configure_logging(__name__)
request_count = 0

HEADERS = {"User-Agent": "TaylorDataApp/1.0"}


def _on_backoff(details):
    print(f"Retrying after {details['tries']} tries; waited {details['wait']:.2f}s")


@backoff.on_exception(
    backoff.expo,
    (httpx.HTTPStatusError, httpx.TransportError),
    max_time=180,
    giveup=lambda e: isinstance(e, httpx.HTTPStatusError) and e.response.status_code in {400, 403, 404},
    on_backoff=_on_backoff,
)
async def call_one(client: httpx.AsyncClient, type_id: int, length: int, region_id: int, limiter: AsyncLimiter, sema: asyncio.Semaphore) -> dict:
    global request_count

    total_req = length
    async with limiter:
        await asyncio.sleep(random.uniform(0, 0.05))
        async with sema:
            r = await client.get(
                f"https://esi.evetech.net/markets/{region_id}/history",
                headers=HEADERS,
                params={"type_id": str(type_id)},
                timeout=30.0,
            )
            request_count += 1
            print(f"\r fetching history. ({round(100*(request_count/total_req),3)}%)", end="", flush=True)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        await asyncio.sleep(float(ra))
                    except ValueError:
                        pass
                r.raise_for_status()
            r.raise_for_status()
            return {"type_id": type_id, "data": r.json()}


async def async_history(watchlist: list[int] = None, region_id: int = None):
    # Default to primary region if none specified
    if region_id is None:
        region_id = ESIConfig("primary").region_id

    if watchlist is None:
        watchlist = DatabaseConfig("wcmkt").get_watchlist()
        type_ids = watchlist["type_id"].unique().tolist()
    else:
        type_ids = watchlist

    length = len(type_ids)

    # Create limiter and semaphore within the async function to avoid event loop issues
    limiter = AsyncLimiter(300, time_period=60.0)
    sema = asyncio.Semaphore(50)

    t0 = time.perf_counter()
    async with httpx.AsyncClient(http2=True) as client:
        results = await asyncio.gather(*(call_one(client, tid, length, region_id, limiter, sema) for tid in type_ids))
    logger.info(f"Got {len(results)} results in {time.perf_counter()-t0:.1f}s")
    logger.info(f"Request count: {request_count}")
    return results


def run_async_history(watchlist: list[int] = None, region_id: int = None):
    return asyncio.run(async_history(watchlist, region_id))


def process_jita_history_data(results: list) -> list[JitaHistory]:
    """Process raw API results into JitaHistory model instances"""
    processed_records = []
    timestamp = datetime.now()

    for result in results:
        type_id = result["type_id"]
        type_name = get_type_name(type_id)

        for day_data in result["data"]:
            record = JitaHistory(
                date=datetime.strptime(day_data["date"], "%Y-%m-%d"),
                type_name=type_name,
                type_id=str(type_id),
                average=day_data["average"],
                volume=day_data["volume"],
                highest=day_data["highest"],
                lowest=day_data["lowest"],
                order_count=day_data["order_count"],
                timestamp=timestamp
            )
            processed_records.append(record)

    return processed_records


# Convenience function for fetching Jita (The Forge) history
def run_async_jita_history(watchlist: list[int] = None):
    """Fetch history from The Forge region (Jita) and return processed JitaHistory records"""
    THE_FORGE_REGION_ID = 10000002
    results = asyncio.run(async_history(watchlist, THE_FORGE_REGION_ID))
    return process_jita_history_data(results)


if __name__ == "__main__":
    pass
