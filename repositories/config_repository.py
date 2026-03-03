"""Repository adapter for configuration storage."""

from __future__ import annotations


class ConfigRepository:
    """Repository facade around the configuration engine."""

    def __init__(self, config_engine):
        self.config_engine = config_engine

    def get_guild(self, guild_id: str) -> dict:
        """Return persisted config object for a guild."""
        return self.config_engine.get_guild(str(guild_id))

    def save_module(self, guild_id: str, module_name: str, payload: dict) -> None:
        """Persist module payload for a guild."""
        self.config_engine.save_module(str(guild_id), str(module_name), dict(payload))

    def get_all(self) -> dict:
        """Return all guild configuration objects."""
        return self.config_engine.get_all()
