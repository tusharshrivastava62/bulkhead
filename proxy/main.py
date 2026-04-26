"""bulkhead proxy with request coalescing and load shedding.

every GET /data/{key} goes through:
1. shedder — capacity-based admission control (returns 503 if at limit)
2. coalescer — dedups concurrent same-key requests on this replica
3. backend fetch
"""

import os
import time

import httpx
from fastapi import FastAPI, HTTPException

from proxy.coalescer import Coalescer
from proxy.shedder import Shedder


app = FastAPI(title="bulkhead")

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:9000")
BACKEND_TIMEOUT = float(os.getenv("BACKEND_TIMEOUT", "5.0"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "50"))

_http_client: httpx.AsyncClient = None
_coalescer = Coalescer()
_shedder = Shedder(max_concurrent=MAX_CONCURRENT)


@app.on_event("startup")
async def startup():
    global _http_client
    _http_client = httpx.AsyncClient(
        base_url=BACKEND_URL,
        timeout=BACKEND_TIMEOUT,
    )


@app.on_event("shutdown")
async def shutdown():
    if _http_client:
        await _http_client.aclose()


@app.get("/health")
async def health():
    return {"status": "ok", "backend": BACKEND_URL}


@app.get("/stats")
async def stats():
    return {
        "coalescer": _coalescer.stats(),
        "shedder": _shedder.stats(),
    }


@app.post("/stats/reset")
async def reset_stats():
    _coalescer.reset_stats()
    _shedder.reset_stats()
    return {"reset": True}


async def _fetch_from_backend(key: str) -> dict:
    try:
        resp = await _http_client.get(f"/data/{key}")
        resp.raise_for_status()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="backend timeout")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"backend error: {e}")
    return resp.json()


@app.get("/data/{key}")
async def proxy_get_data(key: str):
    # admission control — fail fast if over capacity
    if not _shedder.try_acquire():
        raise HTTPException(
            status_code=503,
            detail="shedding: at concurrency limit",
            headers={"Retry-After": "1"},
        )

    start = time.perf_counter()
    try:
        result = await _coalescer.get_or_fetch(
            key=key,
            fetch_fn=lambda: _fetch_from_backend(key),
        )
        elapsed_ms = (time.perf_counter() - start) * 1000
        out = dict(result)
        out["_proxy_latency_ms"] = round(elapsed_ms, 2)
        return out
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        _shedder.release(elapsed_ms)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
