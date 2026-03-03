"""Repository for moderation-related persistence operations."""

from __future__ import annotations


class ModerationRepository:
    """Persist and query moderation warning records."""

    def __init__(self, pool):
        self.pool = pool

    async def add_warning(self, guild_id: int, user_id: int, moderator_id: int, reason: str, created_at: int) -> None:
        await self.pool.execute(
            """
            INSERT INTO moderation_warnings (guild_id, user_id, moderator_id, reason, created_at, active)
            VALUES (?, ?, ?, ?, ?, 1)
            """,
            (str(guild_id), str(user_id), str(moderator_id), str(reason or "")[:500], int(created_at)),
        )

    async def count_active_warnings(self, guild_id: int, user_id: int) -> int:
        row = await self.pool.fetchone(
            "SELECT COUNT(*) AS c FROM moderation_warnings WHERE guild_id=? AND user_id=? AND active=1",
            (str(guild_id), str(user_id)),
        )
        return int((row or {}).get("c", 0))

    async def clear_active_warnings(self, guild_id: int, user_id: int) -> int:
        return await self.pool.execute(
            "UPDATE moderation_warnings SET active=0 WHERE guild_id=? AND user_id=? AND active=1",
            (str(guild_id), str(user_id)),
        )
