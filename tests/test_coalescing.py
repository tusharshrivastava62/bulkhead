#!/usr/bin/env python3
"""test request coalescing.

fires N concurrent requests for the same key. backend should only see 1 call
because the coalescer piggybacks all the duplicates onto the first in-flight.

run this with the full stack running (make up)."""

import asyncio
import sys
import time

import httpx


PROXY_URL = "http://localhost:8080"
BACKEND_URL = "http://localhost:9000"
NUM_CONCURRENT = 100


async def hit_proxy(client, key):
    r = await client.get(f"{PROXY_URL}/data/{key}", timeout=10)
    return r.status_code, r.json()


async def test_coalescing_single_key():
    """N concurrent requests for same key = 1 backend call."""
    print(f"\ntest 1: {NUM_CONCURRENT} concurrent requests for SAME key")
    print("-" * 55)

    # reset backend counters
    httpx.post(f"{BACKEND_URL}/stats/reset")
    httpx.post(f"{PROXY_URL}/stats/reset")

    async with httpx.AsyncClient() as client:
        start = time.perf_counter()
        tasks = [hit_proxy(client, "hot_key") for _ in range(NUM_CONCURRENT)]
        results = await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start

    statuses = [r[0] for r in results]
    all_ok = all(s == 200 for s in statuses)
    # all responses should have same value (they came from 1 backend fetch)
    values = set(r[1].get("value") for r in results)

    backend_stats = httpx.get(f"{BACKEND_URL}/stats").json()
    proxy_stats = httpx.get(f"{PROXY_URL}/stats").json()

    backend_calls = backend_stats["total_calls"]

    print(f"  total requests sent:     {NUM_CONCURRENT}")
    print(f"  all returned 200:        {all_ok}")
    print(f"  unique values returned:  {len(values)} (expected 1)")
    print(f"  backend calls observed:  {backend_calls}")
    print(f"  wall time:               {elapsed:.2f}s")
    print(f"  proxy coalescer stats:   {proxy_stats}")

    # the core assertion: N requests → way fewer backend calls
    # (not always exactly 1 because of timing — but should be dramatically less)
    if backend_calls >= NUM_CONCURRENT / 2:
        print(f"  FAIL: coalescing didn't collapse requests")
        return False
    print(f"  PASS: fan-out ratio {NUM_CONCURRENT / max(backend_calls, 1):.1f}:1")
    return True


async def test_no_coalescing_different_keys():
    """N concurrent requests for DIFFERENT keys = N backend calls."""
    print(f"\ntest 2: {NUM_CONCURRENT} concurrent requests for DIFFERENT keys")
    print("-" * 55)

    httpx.post(f"{BACKEND_URL}/stats/reset")
    httpx.post(f"{PROXY_URL}/stats/reset")

    async with httpx.AsyncClient() as client:
        tasks = [hit_proxy(client, f"unique_{i}") for i in range(NUM_CONCURRENT)]
        results = await asyncio.gather(*tasks)

    backend_stats = httpx.get(f"{BACKEND_URL}/stats").json()
    backend_calls = backend_stats["total_calls"]

    print(f"  total requests sent:     {NUM_CONCURRENT}")
    print(f"  backend calls observed:  {backend_calls}")

    if backend_calls != NUM_CONCURRENT:
        print(f"  FAIL: unique keys should all hit backend")
        return False
    print(f"  PASS: different keys correctly bypass coalescing")
    return True


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
    else:
        print("services not responding — is the stack up?")
        sys.exit(1)

    ok1 = await test_coalescing_single_key()
    ok2 = await test_no_coalescing_different_keys()

    if ok1 and ok2:
        print("\nALL OK")
    else:
        print("\nFAILED")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
