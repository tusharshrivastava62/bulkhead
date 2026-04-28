"""
Priority tier extraction from request headers.

Tiers:
  1 = critical   (always passes shedding, used as latency probe traffic)
  2 = normal     (default, sheds when capacity > 70%)
  3 = batch      (first to shed, lowest priority)

Header: X-Priority: 1 | 2 | 3
Default if header absent or invalid: 2 (normal)
"""
from typing import Optional


TIER_CRITICAL = 1
TIER_NORMAL = 2
TIER_BATCH = 3
DEFAULT_TIER = TIER_NORMAL

VALID_TIERS = {TIER_CRITICAL, TIER_NORMAL, TIER_BATCH}


def parse_priority(header_value: Optional[str]) -> int:
    """
    Parse X-Priority header. Returns tier int.
    Falls back to DEFAULT_TIER on missing or malformed input.
    """
    if header_value is None:
        return DEFAULT_TIER
    try:
        tier = int(header_value.strip())
    except (ValueError, AttributeError):
        return DEFAULT_TIER
    if tier not in VALID_TIERS:
        return DEFAULT_TIER
    return tier


def tier_name(tier: int) -> str:
    return {
        TIER_CRITICAL: "critical",
        TIER_NORMAL: "normal",
        TIER_BATCH: "batch",
    }.get(tier, "unknown")
