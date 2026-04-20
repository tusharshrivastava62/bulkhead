"""bulkhead proxy. fronts a backend service with adaptive load shedding
and request coalescing. this is just the basic forwarding layer — coalescing
and shedding come in later commits."""

import os
import time

import httpx
from fastapi import FastAPI, HTTPException, Request


app = FastAPI(title="bulkhead")

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:9000")
BACKEND_TIMEOUT = float(os.getenv("BACKEND_TIMEOUT", "5.0"))

# single shared client for connection pooling
_http_client: httpx.AsyncClient = None


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


@app.get("/data/{key}")
async def proxy_get_data(key: str):
    """forwards to backend. no coalescing yet."""
    start = time.perf_counter()
    try:
        resp = await _http_client.get(f"/data/{key}")
        resp.raise_for_status()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="backend timeout")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"backend error: {e}")

    elapsed_ms = (time.perf_counter() - start) * 1000
    result = resp.json()
    result["_proxy_latency_ms"] = round(elapsed_ms, 2)
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
