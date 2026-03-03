"""Custom command service with cooldown and sanitization gates."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict


class CustomCommandService:
    """Coordinate custom command sync plus per-command cooldown checks."""

    def __init__(self, manager, cooldown_sec: int = 3):
        self.manager = manager
        self.cooldown_sec = max(1, int(cooldown_sec))
        self._lock = asyncio.Lock()
        self._cooldowns: dict[tuple[int, int, str], float] = defaultdict(float)

    async def sync(self, tree, config_engine):
        """Rebuild custom command registrations from persisted configuration."""
        await self.manager.sync(tree, config_engine)

    async def allow_execution(self, guild_id: int, user_id: int, command_name: str) -> tuple[bool, int]:
        """Check and apply per-user command cooldown for custom command execution."""
        now = time.time()
        key = (int(guild_id), int(user_id), str(command_name))
        async with self._lock:
            until = float(self._cooldowns.get(key, 0.0))
            if now < until:
                return False, max(1, int(until - now))
            self._cooldowns[key] = now + self.cooldown_sec
            if len(self._cooldowns) > 20000:
                cutoff = now - (self.cooldown_sec * 2)
                stale = [k for k, v in self._cooldowns.items() if v < cutoff]
                for stale_key in stale:
                    self._cooldowns.pop(stale_key, None)
            return True, 0
