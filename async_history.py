import asyncio, random, time
import httpx
from aiolimiter import AsyncLimiter
import backoff
from config import DatabaseConfig,ESIConfig
from logging_config import configure_logging

url = ESIConfig("primary").market_history_url

logger = configure_logging(__name__)
request_count = 0


# Respect 300 requests per minute = 5/sec
limiter = AsyncLimiter(300, time_period=60.0)
sema = asyncio.Semaphore(50)  # controls socket concurrency

HEADERS = {"User-Agent": "TaylorDataApp/1.0"}  # customize

def _on_backoff(details):
    print(f"Retrying after {details['tries']} tries; waited {details['wait']:.2f}s")

@backoff.on_exception(
    backoff.expo,
    (httpx.HTTPStatusError, httpx.TransportError),
    max_time=180,
    giveup=lambda e: isinstance(e, httpx.HTTPStatusError) and e.response.status_code in {400, 403, 404},
    on_backoff=_on_backoff,
)
async def call_one(client: httpx.AsyncClient, type_id: int, length: int) -> dict:
    global request_count

    logger.info(f"Fetching history for {type_id}")
    async with limiter:   # enforces 300/minute
        await asyncio.sleep(random.uniform(0, 0.05))  # jitter
        async with sema:
            r = await client.get(
                f"{url}",
                headers=HEADERS,
                params={"type_id": str(type_id)},
                timeout=30.0,
            )
            request_count += 1
            logger.info(f"Response: {r.status_code}, request count: {request_count}/{length}")
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        await asyncio.sleep(float(ra))
                    except ValueError:
                        pass
                r.raise_for_status()
            r.raise_for_status()
            return r.json()

async def async_history():
    watchlist = DatabaseConfig("wcmkt3").get_watchlist()
    type_ids = watchlist["type_id"].unique().tolist() # example list of 835 IDs
    length = len(type_ids)
    t0 = time.perf_counter()
    async with httpx.AsyncClient(http2=True) as client:
        results = await asyncio.gather(*(call_one(client, tid, length) for tid in type_ids))
    print(f"Got {len(results)} results in {time.perf_counter()-t0:.1f}s")
    print(f"Request count: {request_count}")

def run_async_history():
    asyncio.run(async_history())

if __name__ == "__main__":
    pass