"""concurrency-limit load shedder.

shed decision is based on a hard cap on in-flight requests per replica.
when at capacity, new requests are immediately rejected with a 503-equivalent.

latency is also tracked (rolling p99) for observability via /stats. it does
NOT yet drive shedding decisions — doing that correctly requires probe
traffic handling for recovery, which lives with priority tiers in Day 4.
"""

from proxy.latency_tracker import LatencyTracker


class Shedder:
    def __init__(
        self,
        max_concurrent: int = 50,
        latency_window: int = 200,
    ):
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be at least 1")
        self.max_concurrent = max_concurrent
        self._latency = LatencyTracker(window_size=latency_window)
        self._inflight = 0
        # metrics
        self._accepted = 0
        self._shed_capacity = 0

    def try_acquire(self) -> bool:
        """non-blocking. returns False if at capacity. asyncio-safe (no await)."""
        if self._inflight >= self.max_concurrent:
            self._shed_capacity += 1
            return False
        self._inflight += 1
        self._accepted += 1
        return True

    def release(self, latency_ms: float) -> None:
        # _inflight may not match _accepted across resets; clamp to >= 0
        if self._inflight > 0:
            self._inflight -= 1
        self._latency.record(latency_ms)

    def stats(self) -> dict:
        total = self._accepted + self._shed_capacity
        return {
            "inflight": self._inflight,
            "max_concurrent": self.max_concurrent,
            "accepted": self._accepted,
            "shed_capacity": self._shed_capacity,
            "accept_rate": round(self._accepted / total, 3) if total else 0.0,
            "latency": self._latency.stats(),
        }

    def reset_stats(self) -> None:
        self._accepted = 0
        self._shed_capacity = 0
        self._latency.reset()
