"""bulkhead proxy with request coalescing.

every GET /data/{key} goes through the coalescer first. if another request
for the same key is already in flight, we wait on its asyncio.Event and
piggyback on its response. one backend call serves many proxy clients."""

import os
import time

import httpx
from fastapi import FastAPI, HTTPException

from proxy.coalescer import Coalescer


app = FastAPI(title="bulkhead")

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:9000")
BACKEND_TIMEOUT = float(os.getenv("BACKEND_TIMEOUT", "5.0"))

# single shared http client for connection pooling
_http_client: httpx.AsyncClient = None
_coalescer = Coalescer()


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
    return _coalescer.stats()


@app.post("/stats/reset")
async def reset_stats():
    _coalescer.reset_stats()
    return {"reset": True}


async def _fetch_from_backend(key: str) -> dict:
    """the actual backend call. coalescer wraps this."""
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
    start = time.perf_counter()

    result = await _coalescer.get_or_fetch(
        key=key,
        fetch_fn=lambda: _fetch_from_backend(key),
    )

    elapsed_ms = (time.perf_counter() - start) * 1000
    # copy so we don't mutate the shared coalesced result
    out = dict(result)
    out["_proxy_latency_ms"] = round(elapsed_ms, 2)
    return out


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
