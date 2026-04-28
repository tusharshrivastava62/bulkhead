"""
Capacity + latency-based load shedder with priority tiers and hysteresis.

Decision logic per tier:
  Tier 1 (critical): always admit. Used as latency probe traffic.
  Tier 2 (normal):   shed when capacity >= 70% OR latency-shedding active.
  Tier 3 (batch):    shed when capacity >= 50% OR latency-shedding active.

Latency-based shedding (hysteresis):
  - Activates when tier-1 p99 > LATENCY_HIGH_MS
  - Deactivates only when tier-1 p99 < LATENCY_LOW_MS
  - Hysteresis gap prevents oscillation around threshold

Why tier-1 probes matter:
  Naive latency shedding has a recovery problem — if all traffic gets shed,
  no new latency samples come in, so p99 stays "high" forever and recovery
  never triggers. Tier-1 always passes, so we always have fresh latency data
  to detect when the backend has recovered.
"""
from typing import Optional
from proxy.latency_tracker import LatencyTracker
from proxy.priority import TIER_CRITICAL, TIER_NORMAL, TIER_BATCH


# capacity thresholds per tier
TIER2_SHED_THRESHOLD = 0.70
TIER3_SHED_THRESHOLD = 0.50

# latency shedding hysteresis (milliseconds, p99)
LATENCY_HIGH_MS = 200.0   # turn ON latency shedding above this
LATENCY_LOW_MS = 120.0    # turn OFF latency shedding below this
MIN_SAMPLES_FOR_LATENCY_DECISION = 20


class Shedder:
    def __init__(self, max_concurrent: int = 50, latency_window: int = 200):
        self.max_concurrent = max_concurrent
        self._latency_window = latency_window
        self._inflight = 0
        self._accepted = 0
        self._shed_capacity = 0
        self._shed_latency = 0
        self._shed_by_tier = {TIER_CRITICAL: 0, TIER_NORMAL: 0, TIER_BATCH: 0}
        self._accepted_by_tier = {TIER_CRITICAL: 0, TIER_NORMAL: 0, TIER_BATCH: 0}

        # tier-1 latency tracker drives latency shedding decisions
        self.tier1_latency = LatencyTracker(window_size=latency_window)
        # overall latency for observability
        self.latency = LatencyTracker(window_size=latency_window)

        self._latency_shedding_active = False

    def _update_latency_shedding_state(self) -> None:
        """Hysteresis: only flip state when crossing far threshold."""
        stats = self.tier1_latency.stats()
        if stats["count"] < MIN_SAMPLES_FOR_LATENCY_DECISION:
            return
        p99 = stats["p99"]
        if not self._latency_shedding_active and p99 > LATENCY_HIGH_MS:
            self._latency_shedding_active = True
        elif self._latency_shedding_active and p99 < LATENCY_LOW_MS:
            self._latency_shedding_active = False

    def try_acquire(self, tier: int = TIER_NORMAL) -> bool:
        # tier-1 always passes — provides probe traffic for latency observability
        if tier == TIER_CRITICAL:
            self._inflight += 1
            self._accepted += 1
            self._accepted_by_tier[TIER_CRITICAL] += 1
            return True

        utilization = self._inflight / self.max_concurrent if self.max_concurrent > 0 else 1.0

        # latency-based shedding (applies to non-critical tiers when active)
        if self._latency_shedding_active:
            self._shed_latency += 1
            self._shed_by_tier[tier] += 1
            return False

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

    def release(self, latency_ms: float, tier: int = TIER_NORMAL) -> None:
        if self._inflight > 0:
            self._inflight -= 1
        self.latency.record(latency_ms)
        if tier == TIER_CRITICAL:
            self.tier1_latency.record(latency_ms)
        self._update_latency_shedding_state()

    def reset_stats(self) -> None:
        self._accepted = 0
        self._shed_capacity = 0
        self._shed_latency = 0
        self._shed_by_tier = {TIER_CRITICAL: 0, TIER_NORMAL: 0, TIER_BATCH: 0}
        self._accepted_by_tier = {TIER_CRITICAL: 0, TIER_NORMAL: 0, TIER_BATCH: 0}
        self.tier1_latency = LatencyTracker(window_size=self._latency_window)
        self.latency = LatencyTracker(window_size=self._latency_window)
        self._latency_shedding_active = False

    def stats(self) -> dict:
        total_attempts = self._accepted + self._shed_capacity + self._shed_latency
        accept_rate = self._accepted / total_attempts if total_attempts > 0 else 0.0
        return {
            "inflight": self._inflight,
            "max_concurrent": self.max_concurrent,
            "accepted": self._accepted,
            "shed_capacity": self._shed_capacity,
            "shed_latency": self._shed_latency,
            "accept_rate": round(accept_rate, 3),
            "accepted_by_tier": dict(self._accepted_by_tier),
            "shed_by_tier": dict(self._shed_by_tier),
            "latency_shedding_active": self._latency_shedding_active,
            "tier1_latency": self.tier1_latency.stats(),
            "overall_latency": self.latency.stats(),
        }
