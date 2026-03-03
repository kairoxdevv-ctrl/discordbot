import time


class MetricsEngine:
    def __init__(self, repo):
        self.repo = repo

    async def track(self, guild_id: str, event_type: str, payload: dict | None = None):
        await self.repo.add_event(guild_id, event_type, int(time.time()), payload or {})

    async def daily(self, guild_id: str):
        since = int(time.time()) - 86400
        return await self.repo.aggregate_events(guild_id, since)

    async def weekly(self, guild_id: str):
        since = int(time.time()) - (86400 * 7)
        return await self.repo.aggregate_events(guild_id, since)
