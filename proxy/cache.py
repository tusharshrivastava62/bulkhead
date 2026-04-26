"""Redis-backed short-TTL response cache.

reduces backend load on hot keys across replicas. failures are non-fatal —
if redis is unreachable, we just miss-through to backend.

cache scope: per-key, JSON-serialized backend response. TTL is intentionally
short (default 500ms) since backend data may change frequently. coalescer
handles intra-replica dedup; cache handles inter-replica + temporal dedup.
"""

import json
import logging
import os
from typing import Optional

# redis-py 5+ ships native asyncio support
import redis.asyncio as aioredis


log = logging.getLogger(__name__)


class ResponseCache:
    def __init__(
        self,
        redis_url: Optional[str] = None,
        cache_ttl_ms: int = 500,
        prefix: str = "bulkhead:cache:",
    ):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.cache_ttl_ms = cache_ttl_ms
        self.prefix = prefix
        self._client: Optional[aioredis.Redis] = None
        self._hits = 0
        self._misses = 0
        self._errors = 0

    async def connect(self) -> None:
        try:
            self._client = aioredis.from_url(
                self.redis_url,
                decode_responses=True,
                socket_connect_timeout=2.0,
                socket_timeout=1.0,
            )
            # ping to verify
            await self._client.ping()
        except Exception as e:
            log.warning("redis connect failed: %s — running without cache", e)
            self._client = None

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass

    async def get(self, key: str) -> Optional[dict]:
        if self._client is None:
            return None
        try:
            raw = await self._client.get(self.prefix + key)
        except Exception:
            self._errors += 1
            return None
        if raw is None:
            self._misses += 1
            return None
        try:
            self._hits += 1
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            # corrupt cache entry — count as miss, don't crash
            self._errors += 1
            return None

    async def set(self, key: str, value: dict) -> None:
        if self._client is None:
            return
        try:
            await self._client.set(
                self.prefix + key,
                json.dumps(value),
                px=self.cache_ttl_ms,
            )
        except Exception:
            self._errors += 1

    def stats(self) -> dict:
        total = self._hits + self._misses
        return {
            "connected": self._client is not None,
            "hits": self._hits,
            "misses": self._misses,
            "errors": self._errors,
            "hit_rate": round(self._hits / total, 3) if total else 0.0,
            "ttl_ms": self.cache_ttl_ms,
        }

    def reset_stats(self) -> None:
        self._hits = 0
        self._misses = 0
        self._errors = 0
