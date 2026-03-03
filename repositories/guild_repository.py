"""Repository for guild-scoped audit/history operations."""

from __future__ import annotations

import json


class GuildRepository:
    """Persist guild scoped audit events and fetch audit timeline."""

    def __init__(self, pool):
        self.pool = pool

    async def add_audit_event(
        self,
        guild_id: str,
        actor_id: str,
        actor_name: str,
        action_type: str,
        target_id: str,
        metadata: dict,
        created_at: int,
    ) -> None:
        await self.pool.execute(
            """
            INSERT INTO audit_events (guild_id, actor_id, actor_name, action_type, target_id, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(guild_id),
                str(actor_id),
                str(actor_name)[:120],
                str(action_type)[:120],
                str(target_id)[:120],
                json.dumps(metadata or {}, ensure_ascii=False),
                int(created_at),
            ),
        )

    async def recent_audit_events(self, guild_id: str, limit: int = 200) -> list[dict]:
        lim = max(1, min(500, int(limit)))
        return await self.pool.fetchall(
            """
            SELECT id, guild_id, actor_id, actor_name, action_type, target_id, metadata, created_at
            FROM audit_events
            WHERE guild_id=?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (str(guild_id), lim),
        )
