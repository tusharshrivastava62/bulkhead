"""request coalescer. when N concurrent requests come in for the same key,
only 1 goes to the backend. the others wait on an asyncio.Event and get
the same result.

this is the thundering herd fix — classic pattern from CDNs and caches.
"""

import asyncio
import time
from typing import Any, Callable, Dict, Optional


class InFlight:
    """tracks a request currently going to the backend."""

    def __init__(self):
        self.event = asyncio.Event()
        self.result: Optional[Any] = None
        self.exception: Optional[BaseException] = None
        self.started_at = time.perf_counter()
        self.waiters = 0


class Coalescer:
    """in-memory, per-replica request coalescer.

    cross-replica coalescing needs Redis — that comes in Day 3."""

    def __init__(self):
        self._inflight: Dict[str, InFlight] = {}
        self._lock = asyncio.Lock()
        self._coalesced_count = 0
        self._fresh_count = 0
        self._max_waiters = 0

    async def get_or_fetch(self, key: str, fetch_fn: Callable) -> Any:
        existing_inflight = None
        is_owner = False

        async with self._lock:
            existing = self._inflight.get(key)
            if existing is not None:
                existing.waiters += 1
                self._coalesced_count += 1
                if existing.waiters > self._max_waiters:
                    self._max_waiters = existing.waiters
                existing_inflight = existing
            else:
                existing_inflight = InFlight()
                self._inflight[key] = existing_inflight
                self._fresh_count += 1
                is_owner = True

        if is_owner:
            return await self._do_fetch(key, existing_inflight, fetch_fn)

        await existing_inflight.event.wait()
        if existing_inflight.exception is not None:
            raise existing_inflight.exception
        return existing_inflight.result

    async def _do_fetch(self, key: str, inflight: InFlight, fetch_fn: Callable) -> Any:
        try:
            result = await fetch_fn()
            inflight.result = result
            return result
        except BaseException as e:
            inflight.exception = e
            raise
        finally:
            # remove from dict FIRST, then notify waiters.
            # if we set the event before the dict delete, a new request could
            # see the in-flight entry, attach as waiter, wait forever because
            # the event was already set and cleared by the previous wait. 
            # (actually asyncio.Event.wait returns immediately if already set,
            # but the waiter would then get stale result from a cleaned entry)
            async with self._lock:
                if self._inflight.get(key) is inflight:
                    del self._inflight[key]
            inflight.event.set()

    def stats(self) -> dict:
        total = self._coalesced_count + self._fresh_count
        fan_out_ratio = 0.0
        if self._fresh_count > 0:
            fan_out_ratio = total / self._fresh_count
        return {
            "total_requests": total,
            "fresh_fetches": self._fresh_count,
            "coalesced": self._coalesced_count,
            "max_concurrent_waiters": self._max_waiters,
            "fan_out_ratio": round(fan_out_ratio, 2),
            "inflight_now": len(self._inflight),
        }

    def reset_stats(self):
        self._coalesced_count = 0
        self._fresh_count = 0
        self._max_waiters = 0
