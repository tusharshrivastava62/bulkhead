asyncio proxy with priority-tier admission control, load shedding, request 
coalescing, and Redis cache. built to protect a backend service from 
thundering herds and overload — without external coordination.

## benchmark results

**sustained load — locust 30s, 100 users (10% critical / 60% normal / 30% batch)**

| metric | value |
|---|---|
| total requests | 12,050 |
| failures | 0 (0%) |
| throughput | 402 req/s sustained |
| cache hit rate | 86.4% (3,442 / 3,984 fetches) |
| p99 latency tier-1 | 76ms |
| p99 latency tier-2 | 73ms |
| p99 latency tier-3 | 72ms |

**priority stress test — 600 mixed requests in 4.59s**

| tier | sent | accepted | shed |
|---|---|---|---|
| 1 (critical) | 50 | 50 (100%) | 0 (0%) |
| 2 (normal) | 250 | 250 (100%) | 0 (0%) |
| 3 (batch) | 300 | 218 (72.7%) | 82 (27.3%) |

tier-1 admitted 100% while tier-3 absorbed 27% shedding under the same load.

## architecture
internet → nginx (port 8080, round-robin)
↓        ↓        ↓
proxy1   proxy2   proxy3   (each runs all 4 layers)
↓        ↓        ↓
Redis (shared response cache)
↓
backend (the protected service)

each proxy runs 4 layers in sequence on every request:

1. **priority middleware** — reads `X-Priority` header, stamps request as 
   tier 1, 2, or 3
2. **shedder** — checks inflight count against per-tier thresholds. also 
   watches tier-1 p99 latency with hysteresis (ON at 200ms, OFF at 120ms)
3. **cache** — checks Redis for a cached response (500ms TTL)
4. **coalescer** — if 100 requests arrive for the same key simultaneously, 
   makes 1 backend call and shares the result with all 100

## how it works

a request arrives and passes through all 4 layers:

- **tier stamp** — priority middleware reads the header. missing header 
  defaults to tier 3.
- **shedder check** — tier 1 always passes. tier 2 sheds above 70% capacity. 
  tier 3 sheds above 50% capacity. shed requests get HTTP 503 with 
  `Retry-After: 1` in ~1ms. backend never sees them.
- **cache check** — if Redis has the key and TTL is valid, return immediately. 
  86.4% of requests were answered this way in the locust run.
- **coalescer** — if a backend call is already in flight for this key, park 
  the new request on an asyncio.Event. when the owner finishes, all parked 
  requests wake up with the same result. 158 requests were coalesced in the 
  30s locust run.

## design decisions

**tier-1 as probe traffic, not just admission bypass**

naive latency-based shedding has a recovery problem: if all traffic gets shed, 
no new latency samples arrive, p99 stays elevated, and the system never 
recovers. tier-1 traffic always passes the shedder — this gives continuous 
backend samples even under maximum load. the shedder watches tier-1 p99 
specifically to decide when to activate and deactivate latency shedding.

**hysteresis on latency shedding (200ms ON / 120ms OFF)**

a single threshold causes oscillation — shedding activates, load drops, 
threshold clears, shedding deactivates, load rises, repeat. the 80ms gap 
between activation and deactivation thresholds prevents this flapping.

**per-tier capacity thresholds instead of a single global limit**

a global limit treats a payment confirmation the same as a background batch 
job. per-tier thresholds (50% for tier-3, 70% for tier-2, never for tier-1) 
mean low-priority work absorbs overload before it touches critical paths.

**shared Redis cache across all 3 proxy replicas**

if each proxy cached independently, proxy 1 fetching product_123 wouldn't 
help proxy 2 handle the next request for the same key. shared Redis means one 
backend call benefits all three replicas for 500ms.

**coalescer uses asyncio.Event with deletion before set**

the inflight dict entry is deleted before the event fires. if deletion 
happened after, a new arrival between fire and deletion would attach to a 
stale completed entry and get the wrong result. subtle ordering that prevents 
a race under high concurrency.

## running it

**requirements:** docker, docker-compose, python 3.9+

```bash
# start the stack
docker-compose up -d --build

# run sustained load test (requires locust)
pip install locust
locust -f tests/locustfile.py --headless -u 100 -r 25 -t 30s \
  --host http://localhost:8080

# run priority tier stress test
python3 tests/test_priority.py

# check live stats
curl http://localhost:8080/stats | python3 -m json.tool

# tear down
docker-compose down
```

the `/stats` endpoint returns real-time counters for all 4 layers — inflight 
count, shed counts by tier, cache hit rate, tier-1 p99 latency, and whether 
latency shedding is currently active.

## limitations

- **shedder state is per-replica** — each proxy tracks its own inflight count. 
  under nginx round-robin, actual system capacity is `MAX_CONCURRENT × 3`. 
  true global capacity limiting would need a shared counter in Redis.
- **coalescer is per-replica** — two replicas can make simultaneous backend 
  calls for the same key. cross-replica coalescing would require a distributed 
  lock.
- **cache TTL is fixed at 500ms** — no per-key TTL configuration. a 
  production cache would vary TTL by data freshness requirements.
- **no circuit breaker** — if the backend goes down entirely, requests drain 
  through until the shedder fills up. a circuit breaker would fail fast 
  without consuming inflight slots.
- **locust runs from host machine** — gevent build dependency requires a C 
  compiler not present in python:3.9-slim. split into requirements.txt 
  (proxy) and requirements-dev.txt (locust) to keep Docker builds clean.

## what i'd do next

- **distributed shedder** — shared inflight counter in Redis with atomic 
  INCR/DECR so capacity limits are truly global across all replicas
- **per-backend circuit breaker** — track backend failure rate per instance, 
  open circuit after threshold, half-open probe with tier-1 traffic
- **consistent hashing for cache** — route requests for the same key to the 
  same proxy replica to improve cache locality before hitting Redis
- **adaptive TTL** — vary cache TTL based on observed backend response 
  stability rather than fixed 500ms

## lessons learned

**the gevent Docker build failure** — adding locust to requirements.txt broke 
the proxy Docker build because gevent needs a C compiler not present in 
python:3.9-slim. fix was splitting dependencies: proxy deps in 
requirements.txt go into Docker, locust in requirements-dev.txt stays on the 
host. Docker layer caching masked the failure initially — `--no-cache` 
exposed it.

**cache class name mismatch** — initial Day 4 main.py imported `Cache` but 
the real class was `ResponseCache`. the error only surfaced at runtime, not 
import time, because the object was instantiated inside a function. reading 
the actual source with `git show` rather than relying on memory fixed it in 
one pass.

**tier-1 probe traffic solves a problem that isn't obvious until you think 
about recovery** — the original shedder design had no answer for "how does 
the system know when to stop shedding." building the solution into the 
admission policy rather than adding a separate health check endpoint kept the 
design clean and eliminated an entire class of failure.