"""Abuse/rate-limiting service with TTL cleanup and backend abstraction."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque


class InMemoryAbuseBackend:
    """In-memory backend for command rate limiting."""

    def __init__(self):
        self._user_hits: dict[tuple[int, int, str], deque[float]] = defaultdict(deque)
        self._guild_hits: dict[tuple[int, str], deque[float]] = defaultdict(deque)


class AbuseService:
    """Centralized abuse guard prepared for future distributed backend."""

    def __init__(
        self,
        backend: InMemoryAbuseBackend | None = None,
        per_user_limit: int = 12,
        per_guild_limit: int = 80,
        window_sec: int = 30,
        burst_limit: int = 6,
        burst_window_sec: int = 5,
    ):
        self.backend = backend or InMemoryAbuseBackend()
        self.per_user_limit = max(1, int(per_user_limit))
        self.per_guild_limit = max(1, int(per_guild_limit))
        self.window_sec = max(2, int(window_sec))
        self.burst_limit = max(1, int(burst_limit))
        self.burst_window_sec = max(1, int(burst_window_sec))
        self._lock = asyncio.Lock()
        self._last_cleanup = 0.0

    async def allow(self, guild_id: int, user_id: int, command_name: str) -> tuple[bool, int]:
        """Evaluate whether a command invocation is allowed under rate/burst limits."""
        now = time.time()
        user_key = (int(guild_id), int(user_id), str(command_name))
        guild_key = (int(guild_id), str(command_name))

        async with self._lock:
            await self._cleanup(now)
            user_q = self.backend._user_hits[user_key]
            guild_q = self.backend._guild_hits[guild_key]
            self._trim(user_q, now, self.window_sec)
            self._trim(guild_q, now, self.window_sec)

            burst_count = self._count_since(user_q, now - self.burst_window_sec)
            if burst_count >= self.burst_limit:
                oldest = user_q[0] if user_q else now
                return False, max(1, int(self.burst_window_sec - (now - oldest)))

            if len(user_q) >= self.per_user_limit or len(guild_q) >= self.per_guild_limit:
                oldest = user_q[0] if user_q else (guild_q[0] if guild_q else now)
                return False, max(1, int(self.window_sec - (now - oldest)))

            user_q.append(now)
            guild_q.append(now)
            return True, 0

    def _trim(self, bucket: deque[float], now: float, window: int) -> None:
        cutoff = now - float(window)
        while bucket and bucket[0] < cutoff:
            bucket.popleft()

    def _count_since(self, bucket: deque[float], cutoff: float) -> int:
        return sum(1 for ts in bucket if ts >= cutoff)

    async def _cleanup(self, now: float) -> None:
        if (now - self._last_cleanup) < 300:
            return
        user_cutoff = now - float(self.window_sec)
        guild_cutoff = now - float(self.window_sec)

        stale_users = []
        for key, hits in self.backend._user_hits.items():
            while hits and hits[0] < user_cutoff:
                hits.popleft()
            if not hits:
                stale_users.append(key)
        for key in stale_users:
            self.backend._user_hits.pop(key, None)

        stale_guilds = []
        for key, hits in self.backend._guild_hits.items():
            while hits and hits[0] < guild_cutoff:
                hits.popleft()
            if not hits:
                stale_guilds.append(key)
        for key in stale_guilds:
            self.backend._guild_hits.pop(key, None)

        self._last_cleanup = now
