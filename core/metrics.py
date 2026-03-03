"""Core layer: metrics ingestion and aggregation facade."""

import time


class MetricsEngine:
    """Core metrics facade writing events and serving aggregated windows."""

    def __init__(self, repo):
        self.repo = repo

    async def track(self, guild_id: str, event_type: str, payload: dict | None = None):
        """Persist a single metrics event for a guild."""
        await self.repo.add_event(guild_id, event_type, int(time.time()), payload or {})

    async def daily(self, guild_id: str):
        """Return event counts for last 24 hours."""
        since = int(time.time()) - 86400
        return await self.repo.aggregate_events(guild_id, since)

    async def weekly(self, guild_id: str):
        """Return event counts for last 7 days."""
        since = int(time.time()) - (86400 * 7)
        return await self.repo.aggregate_events(guild_id, since)
