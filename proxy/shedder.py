"""
Capacity-based load shedder with priority tier awareness.

Decision logic per tier:
  Tier 1 (critical): always admit. Used as latency probe traffic.
  Tier 2 (normal):   shed when inflight >= 70% of max_concurrent.
  Tier 3 (batch):    shed when inflight >= 50% of max_concurrent.

This protects the backend by shedding low-priority work first,
and ensures tier-1 traffic always provides latency samples for
the latency tracker (used by latency-based shedding).
"""
from typing import Optional
from proxy.latency_tracker import LatencyTracker
from proxy.priority import TIER_CRITICAL, TIER_NORMAL, TIER_BATCH


# fraction of capacity at which each tier starts being shed
TIER2_SHED_THRESHOLD = 0.70
TIER3_SHED_THRESHOLD = 0.50


class Shedder:
    def __init__(self, max_concurrent: int = 50, latency_window: int = 200):
        self.max_concurrent = max_concurrent
        self._inflight = 0
        self._accepted = 0
        self._shed_capacity = 0
        self._shed_by_tier = {TIER_CRITICAL: 0, TIER_NORMAL: 0, TIER_BATCH: 0}
        self._accepted_by_tier = {TIER_CRITICAL: 0, TIER_NORMAL: 0, TIER_BATCH: 0}
        self.latency = LatencyTracker(window_size=latency_window)

    def try_acquire(self, tier: int = TIER_NORMAL) -> bool:
        """
        Returns True if request is admitted, False if shed.
        Caller MUST call release() with measured latency if True.
        """
        # tier-1 always passes — provides probe traffic for latency observability
        if tier == TIER_CRITICAL:
            self._inflight += 1
            self._accepted += 1
            self._accepted_by_tier[TIER_CRITICAL] += 1
            return True

        utilization = self._inflight / self.max_concurrent if self.max_concurrent > 0 else 1.0

        # tier-2: shed if at or above 70% capacity
        if tier == TIER_NORMAL and utilization >= TIER2_SHED_THRESHOLD:
            self._shed_capacity += 1
            self._shed_by_tier[TIER_NORMAL] += 1
            return False

        # tier-3: shed first, at 50% capacity
        if tier == TIER_BATCH and utilization >= TIER3_SHED_THRESHOLD:
            self._shed_capacity += 1
            self._shed_by_tier[TIER_BATCH] += 1
            return False

        # also shed any non-critical request if at hard capacity
        if self._inflight >= self.max_concurrent:
            self._shed_capacity += 1
            self._shed_by_tier[tier] += 1
            return False

        self._inflight += 1
        self._accepted += 1
        self._accepted_by_tier[tier] += 1
        return True

    def release(self, latency_ms: float) -> None:
        if self._inflight > 0:
            self._inflight -= 1
        self.latency.record(latency_ms)

    def reset_stats(self) -> None:
        self._accepted = 0
        self._shed_capacity = 0
        self._shed_by_tier = {TIER_CRITICAL: 0, TIER_NORMAL: 0, TIER_BATCH: 0}
        self._accepted_by_tier = {TIER_CRITICAL: 0, TIER_NORMAL: 0, TIER_BATCH: 0}
        self.latency = LatencyTracker(window_size=self.latency.window_size if hasattr(self.latency, 'window_size') else 200)

    def stats(self) -> dict:
        total_attempts = self._accepted + self._shed_capacity
        accept_rate = self._accepted / total_attempts if total_attempts > 0 else 0.0
        return {
            "inflight": self._inflight,
            "max_concurrent": self.max_concurrent,
            "accepted": self._accepted,
            "shed_capacity": self._shed_capacity,
            "accept_rate": round(accept_rate, 3),
            "accepted_by_tier": dict(self._accepted_by_tier),
            "shed_by_tier": dict(self._shed_by_tier),
            "latency": self.latency.stats(),
        }
