#!/usr/bin/env python3
"""burst benchmark. measures fan-out ratio at different concurrency levels.

at each level: fire N concurrent requests for a single key, count how many
actually reach the backend. higher ratio = better thundering herd collapse.

the nginx round-robin means requests land on different replicas, so our
per-replica coalescing only collapses the fraction hitting the same replica.
single-replica is the best-case for coalescing."""

import asyncio
import sys
import time

import httpx


PROXY_URL = "http://localhost:8080"
BACKEND_URL = "http://localhost:9000"

# single proxy replica direct hit (bypasses nginx) for best-case measurement
SINGLE_PROXY_URL = "http://localhost:8001"  # exposed proxy1 directly for test

CONCURRENCY_LEVELS = [10, 50, 100, 500, 1000]


async def hit(client, url, key):
    try:
        r = await client.get(f"{url}/data/{key}", timeout=30)
        return r.status_code
    except Exception:
        return 0


async def run_burst(concurrency: int, url: str, label: str):
    httpx.post(f"{BACKEND_URL}/stats/reset")

    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=2000)) as client:
        start = time.perf_counter()
        tasks = [hit(client, url, "burst_key") for _ in range(concurrency)]
        results = await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start

    success = sum(1 for r in results if r == 200)
    backend_stats = httpx.get(f"{BACKEND_URL}/stats").json()
    backend_calls = backend_stats["total_calls"]
    fan_out = concurrency / max(backend_calls, 1)

    print(f"  {label:<20} N={concurrency:>5}  backend={backend_calls:>5}  "
          f"fan_out={fan_out:>6.1f}:1  success={success}/{concurrency}  "
          f"wall={elapsed:.2f}s")
    return backend_calls, success, elapsed


async def main():
    # wait for services
    for _ in range(30):
        try:
            r = httpx.get(f"{PROXY_URL}/health", timeout=2)
            if r.status_code == 200:
                break
        except httpx.RequestError:
            pass
        await asyncio.sleep(0.5)

    print("=" * 75)
    print("coalescing burst benchmark")
    print("=" * 75)
    print()
    print("via nginx (requests split across 3 replicas):")
    print()

    for c in CONCURRENCY_LEVELS:
        await run_burst(c, PROXY_URL, "nginx")
        await asyncio.sleep(0.5)

    # try to check if single-replica port is exposed
    try:
        r = httpx.get(f"{SINGLE_PROXY_URL}/health", timeout=1)
        if r.status_code == 200:
            print()
            print("direct single-replica (all requests to one replica — best case):")
            print()
            for c in CONCURRENCY_LEVELS:
                await run_burst(c, SINGLE_PROXY_URL, "single_replica")
                await asyncio.sleep(0.5)
    except httpx.RequestError:
        print()
        print("(single-replica port 8001 not exposed — see Day 3 for cross-replica test)")


if __name__ == "__main__":
    asyncio.run(main())
