"""bulkhead proxy with priority-aware capacity + latency shedding,
request coalescing, and Redis cache.

every GET /data/{key} goes through:
1. priority — parse X-Priority header (1=critical, 2=normal, 3=batch)
2. shedder — capacity + latency-based admission (tier-1 always passes)
3. coalescer — dedups concurrent same-key requests on this replica
4. cache check (Redis) — short-TTL response cache shared across replicas
5. backend fetch (only on cache miss)
"""

import os
import time

import httpx
from fastapi import FastAPI, HTTPException, Request

from proxy.cache import ResponseCache
from proxy.coalescer import Coalescer
from proxy.shedder import Shedder
from proxy.priority import parse_priority, tier_name


app = FastAPI(title="bulkhead")

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:9000")
BACKEND_TIMEOUT = float(os.getenv("BACKEND_TIMEOUT", "5.0"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "50"))
CACHE_TTL_MS = int(os.getenv("CACHE_TTL_MS", "500"))

_http_client: httpx.AsyncClient = None
_coalescer = Coalescer()
_shedder = Shedder(max_concurrent=MAX_CONCURRENT)
_cache = ResponseCache(cache_ttl_ms=CACHE_TTL_MS)


@app.on_event("startup")
async def startup():
    global _http_client
    _http_client = httpx.AsyncClient(
        base_url=BACKEND_URL,
        timeout=BACKEND_TIMEOUT,
    )
    await _cache.connect()


@app.on_event("shutdown")
async def shutdown():
    if _http_client:
        await _http_client.aclose()
    await _cache.close()


@app.get("/health")
async def health():
    return {"status": "ok", "backend": BACKEND_URL}


@app.get("/stats")
async def stats():
    return {
        "coalescer": _coalescer.stats(),
        "shedder": _shedder.stats(),
        "cache": _cache.stats(),
    }


@app.post("/stats/reset")
async def reset_stats():
    _coalescer.reset_stats()
    _shedder.reset_stats()
    _cache.reset_stats()
    return {"reset": True}


async def _fetch_with_cache(key: str) -> dict:
    """cache check, then backend on miss. populates cache on success."""
    cached = await _cache.get(key)
    if cached is not None:
        cached = dict(cached)
        cached["_cache_hit"] = True
        return cached

    try:
        resp = await _http_client.get(f"/data/{key}")
        resp.raise_for_status()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="backend timeout")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"backend error: {e}")

    result = resp.json()
    await _cache.set(key, result)
    return result


@app.get("/data/{key}")
async def proxy_get_data(key: str, request: Request):
    tier = parse_priority(request.headers.get("X-Priority"))

    if not _shedder.try_acquire(tier=tier):
        raise HTTPException(
            status_code=503,
            detail=f"shedding: tier {tier} ({tier_name(tier)}) at capacity",
            headers={"Retry-After": "1", "X-Tier": str(tier)},
        )

    start = time.perf_counter()
    try:
        result = await _coalescer.get_or_fetch(
            key=key,
            fetch_fn=lambda: _fetch_with_cache(key),
        )
        elapsed_ms = (time.perf_counter() - start) * 1000
        out = dict(result)
        out["_proxy_latency_ms"] = round(elapsed_ms, 2)
        out["_tier"] = tier
        return out
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        _shedder.release(elapsed_ms, tier=tier)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
