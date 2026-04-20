"""mock backend service. simulates a slow upstream that the proxy fronts.
has adjustable latency via env var and a degraded mode for testing shedding."""

import asyncio
import os
import random
import time
from typing import Optional

from fastapi import FastAPI, HTTPException, Query


app = FastAPI(title="mock-backend")

# knobs for testing
BASE_LATENCY_MS = int(os.getenv("BACKEND_LATENCY_MS", "50"))
JITTER_MS = int(os.getenv("BACKEND_JITTER_MS", "20"))
DEGRADED = os.getenv("BACKEND_DEGRADED", "false").lower() == "true"

# track calls for coalescing verification
_call_count = 0
_call_count_by_key: dict = {}


@app.get("/health")
async def health():
    return {"status": "ok", "degraded": DEGRADED}


@app.get("/stats")
async def stats():
    """used in tests to verify coalescing actually reduces backend calls."""
    return {
        "total_calls": _call_count,
        "by_key": dict(_call_count_by_key),
    }


@app.post("/stats/reset")
async def reset_stats():
    global _call_count
    _call_count = 0
    _call_count_by_key.clear()
    return {"reset": True}


@app.get("/data/{key}")
async def get_data(key: str, extra_slow: bool = Query(False)):
    """simulates a data lookup. takes time. this is what the proxy fronts."""
    global _call_count
    _call_count += 1
    _call_count_by_key[key] = _call_count_by_key.get(key, 0) + 1

    # degraded mode: 5x slower, 10% error rate
    if DEGRADED:
        await asyncio.sleep((BASE_LATENCY_MS * 5) / 1000)
        if random.random() < 0.1:
            raise HTTPException(status_code=503, detail="backend degraded")
    else:
        jitter = random.uniform(-JITTER_MS, JITTER_MS)
        sleep_ms = max(1, BASE_LATENCY_MS + jitter)
        if extra_slow:
            sleep_ms *= 3
        await asyncio.sleep(sleep_ms / 1000)

    return {
        "key": key,
        "value": f"value_for_{key}",
        "timestamp": time.time(),
        "server_call_count": _call_count,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)
