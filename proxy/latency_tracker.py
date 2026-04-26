"""rolling latency tracker. used by shedder to make load-based decisions.

window of recent samples, sorted on read (cheap for small windows).
TODO: replace sort with HDR histogram if window grows.
"""

from collections import deque
from typing import Optional


class LatencyTracker:
    """rolling window of recent latencies."""

    def __init__(self, window_size: int = 200):
        self._samples: deque = deque(maxlen=window_size)
        self._window = window_size

    def record(self, latency_ms: float) -> None:
        self._samples.append(latency_ms)

    def percentile(self, p: float) -> float:
        if not self._samples:
            return 0.0
        if not 0.0 <= p <= 1.0:
            raise ValueError("percentile must be between 0 and 1")
        sorted_samples = sorted(self._samples)
        idx = int(len(sorted_samples) * p)
        return sorted_samples[min(idx, len(sorted_samples) - 1)]

    def p50(self) -> float:
        return self.percentile(0.5)

    def p99(self) -> float:
        return self.percentile(0.99)

    def count(self) -> int:
        return len(self._samples)

    def stats(self) -> dict:
        if not self._samples:
            return {"count": 0, "p50": 0.0, "p99": 0.0, "max": 0.0}
        sorted_s = sorted(self._samples)
        n = len(sorted_s)
        return {
            "count": n,
            "p50": round(sorted_s[n // 2], 2),
            "p99": round(sorted_s[min(int(n * 0.99), n - 1)], 2),
            "max": round(sorted_s[-1], 2),
        }

    def reset(self) -> None:
        self._samples.clear()
