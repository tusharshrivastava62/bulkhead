#!/usr/bin/env python3
"""test capacity-based load shedding under burst.

with MAX_CONCURRENT=50 per replica × 3 replicas = 150 system-wide capacity.
firing 500 concurrent requests with unique keys (no coalescing helping)
should result in ~150 success + ~350 shed (503).

baseline test (50 reqs, under cluster capacity) should have 0 shedding."""

import asyncio
import sys
import time

import httpx


PROXY_URL = "http://localhost:8080"
BACKEND_URL = "http://localhost:9000"


async def hit(client, key):
    try:
        r = await client.get(f"{PROXY_URL}/data/{key}", timeout=10)
        return r.status_code
    except Exception:
        return 0


async def test_under_limit(N=50):
    httpx.post(f"{BACKEND_URL}/stats/reset")
    httpx.post(f"{PROXY_URL}/stats/reset")

    print(f"\ntest 1: {N} concurrent (under cluster capacity 150)")
    print("-" * 55)

    async with httpx.AsyncClient() as client:
        tasks = [hit(client, f"normal_{i}") for i in range(N)]
        results = await asyncio.gather(*tasks)

    accepted = sum(1 for r in results if r == 200)
    shed = sum(1 for r in results if r == 503)

    print(f"  accepted: {accepted}/{N}")
    print(f"  shed:     {shed}")

    if accepted == N and shed == 0:
        print(f"  PASS: no shedding under normal load")
        return True
    print(f"  FAIL: shedder triggered at normal load")
    return False


async def test_over_limit(N=500):
    httpx.post(f"{BACKEND_URL}/stats/reset")
    httpx.post(f"{PROXY_URL}/stats/reset")

    print(f"\ntest 2: {N} concurrent unique keys (no coalescing)")
    print("-" * 55)

    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=2000)) as client:
        start = time.perf_counter()
        tasks = [hit(client, f"shed_unique_{i}") for i in range(N)]
        results = await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start

    accepted = sum(1 for r in results if r == 200)
    shed = sum(1 for r in results if r == 503)
    other = N - accepted - shed

    print(f"  total:      {N}")
    print(f"  accepted:   {accepted} ({accepted/N*100:.1f}%)")
    print(f"  shed (503): {shed} ({shed/N*100:.1f}%)")
    print(f"  other:      {other}")
    print(f"  wall time:  {elapsed:.2f}s")

    backend_calls = httpx.get(f"{BACKEND_URL}/stats").json()["total_calls"]
    print(f"  backend calls: {backend_calls}")

    if shed > 0 and accepted > 0 and other == 0:
        print(f"  PASS: clean shedding — no uncontrolled failures")
        return True
    if shed == 0:
        print(f"  WARN: no shedding triggered (try lower MAX_CONCURRENT or higher N)")
        return False
    print(f"  FAIL: had {other} uncontrolled failures")
    return False


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
        print("services not ready — is the stack up?")
        sys.exit(1)

    ok1 = await test_under_limit()
    ok2 = await test_over_limit()

    if ok1 and ok2:
        print("\nALL OK")
    else:
        print("\nFAILED")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
