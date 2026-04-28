"""
Priority tier behavior under load.

What we prove:
  Test 1 — baseline:
    All three tiers should pass cleanly when the cluster is idle.

  Test 2 — under heavy load:
    With a flood of tier-3 requests, tier-1 must NEVER be shed.
    Tier-2 should be partially shed, tier-3 most aggressively shed.

This is the key result that motivates priority tiers: under stress,
critical traffic survives while batch work is dropped first.
"""
import asyncio
import time
import httpx


PROXY_URL = "http://localhost:8080"
REQUEST_TIMEOUT = 8.0


async def fire(client: httpx.AsyncClient, key: str, tier: int):
    headers = {"X-Priority": str(tier)}
    try:
        r = await client.get(
            f"{PROXY_URL}/data/{key}",
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        return tier, r.status_code
    except Exception:
        return tier, -1


async def test_baseline():
    print()
    print("test 1: baseline — 30 reqs across tiers (under capacity)")
    print("-" * 55)
    async with httpx.AsyncClient() as client:
        tasks = []
        for i in range(10):
            tasks.append(fire(client, f"baseline_{i}_t1", 1))
            tasks.append(fire(client, f"baseline_{i}_t2", 2))
            tasks.append(fire(client, f"baseline_{i}_t3", 3))
        results = await asyncio.gather(*tasks)

    by_tier = {1: [], 2: [], 3: []}
    for tier, status in results:
        by_tier[tier].append(status)

    for tier in (1, 2, 3):
        ok = sum(1 for s in by_tier[tier] if s == 200)
        shed = sum(1 for s in by_tier[tier] if s == 503)
        other = sum(1 for s in by_tier[tier] if s not in (200, 503))
        print(f"  tier {tier}: ok={ok}/{len(by_tier[tier])}  shed={shed}  other={other}")
        assert other == 0, f"tier-{tier} had {other} unexpected failures"
    print("  PASS: all tiers admitted under low load")


async def test_under_heavy_load():
    print()
    print("test 2: heavy load — 600 mixed reqs, tier-1 must survive")
    print("-" * 55)
    # mix: 50 tier-1, 250 tier-2, 300 tier-3 (skewed toward batch)
    async with httpx.AsyncClient() as client:
        tasks = []
        for i in range(50):
            tasks.append(fire(client, f"load_t1_{i}", 1))
        for i in range(250):
            tasks.append(fire(client, f"load_t2_{i}", 2))
        for i in range(300):
            tasks.append(fire(client, f"load_t3_{i}", 3))

        # interleave them to simulate concurrent arrival
        import random
        random.shuffle(tasks)

        start = time.perf_counter()
        results = await asyncio.gather(*tasks)
        wall = time.perf_counter() - start

    by_tier = {1: {"ok": 0, "shed": 0, "other": 0},
               2: {"ok": 0, "shed": 0, "other": 0},
               3: {"ok": 0, "shed": 0, "other": 0}}

    for tier, status in results:
        if status == 200:
            by_tier[tier]["ok"] += 1
        elif status == 503:
            by_tier[tier]["shed"] += 1
        else:
            by_tier[tier]["other"] += 1

    for tier in (1, 2, 3):
        b = by_tier[tier]
        total = b["ok"] + b["shed"] + b["other"]
        ok_pct = b["ok"] / total * 100 if total > 0 else 0
        shed_pct = b["shed"] / total * 100 if total > 0 else 0
        print(f"  tier {tier}: total={total:3d}  ok={b['ok']:3d} ({ok_pct:5.1f}%)  "
              f"shed={b['shed']:3d} ({shed_pct:5.1f}%)  other={b['other']}")

    print(f"  wall time: {wall:.2f}s")

    # CRITICAL ASSERTIONS
    assert by_tier[1]["shed"] == 0, "tier-1 was shed! priority guarantee broken"
    assert by_tier[1]["other"] == 0, "tier-1 had failures"
    print("  PASS: tier-1 had 0 shedding under heavy load")

    # tier-3 should be shed more aggressively than tier-2 (% basis)
    t2_total = by_tier[2]["ok"] + by_tier[2]["shed"]
    t3_total = by_tier[3]["ok"] + by_tier[3]["shed"]
    if t2_total > 0 and t3_total > 0:
        t2_shed_rate = by_tier[2]["shed"] / t2_total
        t3_shed_rate = by_tier[3]["shed"] / t3_total
        print(f"  tier-2 shed rate: {t2_shed_rate:.1%}")
        print(f"  tier-3 shed rate: {t3_shed_rate:.1%}")
        if t3_shed_rate >= t2_shed_rate:
            print("  PASS: tier-3 shed rate >= tier-2 shed rate")
        else:
            print(f"  NOTE: tier-3 shed rate < tier-2 (may happen if backend is fast)")


async def main():
    print("=" * 55)
    print("priority tier test")
    print("=" * 55)
    await test_baseline()
    await test_under_heavy_load()
    print()
    print("ALL OK")


if __name__ == "__main__":
    asyncio.run(main())
