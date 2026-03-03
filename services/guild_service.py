"""Guild/runtime utility service."""

from __future__ import annotations


class GuildService:
    """Read runtime guild state from Discord client."""

    def __init__(self, runtime):
        self.runtime = runtime

    def get_guild(self, guild_id: int):
        """Return guild from runtime cache or None when unavailable."""
        return self.runtime.get_guild(int(guild_id))

    def guild_rows(self) -> list[dict]:
        """Return lightweight guild rows for dashboard selectors."""
        bot = getattr(self.runtime, "bot", None)
        if not bot:
            return []
        rows = [{"id": str(g.id), "name": g.name} for g in sorted(bot.guilds, key=lambda x: x.name.lower())]
        return rows
