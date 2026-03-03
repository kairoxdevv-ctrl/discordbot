"""Configuration service with validation and cache TTL."""

from __future__ import annotations

import copy
import re
import time


_ID_RE = re.compile(r"^\d{5,25}$")


class ConfigService:
    """Validate and persist guild configuration safely."""

    def __init__(self, config_repository, ttl_sec: int = 30):
        self.config_repository = config_repository
        self.ttl_sec = max(1, int(ttl_sec))
        self._cache: dict[str, tuple[int, dict]] = {}

    def get_guild_config(self, guild_id: str) -> dict:
        gid = str(guild_id)
        now = int(time.time())
        cached = self._cache.get(gid)
        if cached and now <= cached[0]:
            return copy.deepcopy(cached[1])
        cfg = self.config_repository.get_guild(gid)
        self._cache[gid] = (now + self.ttl_sec, copy.deepcopy(cfg))
        return copy.deepcopy(cfg)

    def validate_config_structure(self, data: dict) -> dict:
        if not isinstance(data, dict):
            return {}
        out = {}
        for key, value in data.items():
            if isinstance(value, bool):
                out[key] = value
            elif isinstance(value, int):
                out[key] = value
            elif isinstance(value, str):
                v = value.strip()
                out[key] = v[:3000]
            elif isinstance(value, list):
                filtered = []
                for item in value[:300]:
                    s = str(item).strip()
                    if _ID_RE.match(s) or len(s) <= 128:
                        filtered.append(s)
                out[key] = filtered
            elif isinstance(value, dict):
                out[key] = self.validate_config_structure(value)
        return out

    def update_guild_config(self, guild_id: str, module_name: str, patch: dict) -> dict:
        safe_patch = self.validate_config_structure(patch)
        self.config_repository.save_module(str(guild_id), str(module_name), safe_patch)
        self._cache.pop(str(guild_id), None)
        return safe_patch
