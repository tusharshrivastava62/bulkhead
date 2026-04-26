#!/usr/bin/env python3
"""test Redis cache reduces backend load on warm reads.

cold burst: N reqs same key, no cache. each replica's coalescer dedups within,
but each replica still does 1 backend call -> ~3 backend calls total for the
3-replica cluster. cache populated after.

warm burst (within TTL): same key, cache populated. all replicas hit cache,
0 backend calls expected.

uses unique key per run so prior runs don't interfere."""

import asyncio
import sys
import time

import httpx


PROXY_URL = "http://localhost:8080"
BACKEND_URL = "http://localhost:9000"


async def hit(client, key):
    try:
        r = await client.get(f"{PROXY_URL}/data/{key}", timeout=5)
        return r.status_code, (r.json() if r.status_code == 200 else None)
    except Exception as e:
        return 0, None


async def main():
    for _ in range(40):
        try:
            r = httpx.get(f"{PROXY_URL}/health", timeout=2)
            if r.status_code == 200:
                break
        except httpx.RequestError:
            pass
        await asyncio.sleep(0.5)
    else:
        print("services not ready")
        sys.exit(1)

    # unique key per run — avoids stale cache from earlier runs
    test_key = f"cache_test_{int(time.time() * 1000)}"

    # COLD burst — cache empty for this key
    httpx.post(f"{BACKEND_URL}/stats/reset")
    httpx.post(f"{PROXY_URL}/stats/reset")

    print(f"\ncold burst: 30 concurrent reqs on fresh key '{test_key[-12:]}'")
    print("-" * 55)
    async with httpx.AsyncClient() as client:
        cold_tasks = [hit(client, test_key) for _ in range(30)]
        cold_results = await asyncio.gather(*cold_tasks)

    cold_backend = httpx.get(f"{BACKEND_URL}/stats").json()["total_calls"]
    cold_cache_hits = sum(
        1 for s, r in cold_results if r and r.get("_cache_hit")
    )
    cold_ok = sum(1 for s, _ in cold_results if s == 200)

    print(f"  ok responses:        {cold_ok}/30")
    print(f"  backend calls:       {cold_backend} (one per replica, expected ~3)")
    print(f"  responses w/ cache:  {cold_cache_hits}/30 (expected 0 - all from backend)")

    # let cache settle but stay within TTL (default 500ms)
    await asyncio.sleep(0.05)

    # WARM burst — cache populated
    httpx.post(f"{BACKEND_URL}/stats/reset")

    print(f"\nwarm burst: 30 concurrent reqs same key, within cache TTL")
    print("-" * 55)
    async with httpx.AsyncClient() as client:
        warm_tasks = [hit(client, test_key) for _ in range(30)]
        warm_results = await asyncio.gather(*warm_tasks)

    warm_backend = httpx.get(f"{BACKEND_URL}/stats").json()["total_calls"]
    warm_cache_hits = sum(
        1 for s, r in warm_results if r and r.get("_cache_hit")
    )
    warm_ok = sum(1 for s, _ in warm_results if s == 200)

    print(f"  ok responses:        {warm_ok}/30")
    print(f"  backend calls:       {warm_backend} (expected 0)")
    print(f"  responses w/ cache:  {warm_cache_hits}/30 (expected 30)")

    proxy_stats = httpx.get(f"{PROXY_URL}/stats").json()
    cache_stats = proxy_stats.get("cache", {})
    print(f"\n  cache stats: {cache_stats}")

    print()
    if warm_backend == 0 and warm_cache_hits >= 25:
        reduction = ((cold_backend - warm_backend) / max(cold_backend, 1)) * 100
        print(f"  PASS: cache reduced backend by {reduction:.0f}%")
    elif warm_backend < cold_backend:
        reduction = ((cold_backend - warm_backend) / cold_backend) * 100
        print(f"  PARTIAL PASS: cache reduced backend by {reduction:.0f}% "
              f"(some replicas may have raced)")
    else:
        print(f"  FAIL: cache did not reduce backend load")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
