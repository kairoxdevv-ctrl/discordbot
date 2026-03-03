"""Moderation business logic service."""

from __future__ import annotations

import time


class ModerationService:
    """Handle warning lifecycle and moderation audit side-effects."""

    def __init__(self, moderation_repository, guild_repository):
        self.moderation_repository = moderation_repository
        self.guild_repository = guild_repository

    async def add_warning(self, guild_id: int, user_id: int, moderator_id: int, reason: str) -> int:
        now = int(time.time())
        await self.moderation_repository.add_warning(guild_id, user_id, moderator_id, reason, created_at=now)
        return await self.moderation_repository.count_active_warnings(guild_id, user_id)

    async def count_active_warnings(self, guild_id: int, user_id: int) -> int:
        return await self.moderation_repository.count_active_warnings(guild_id, user_id)

    async def clear_active_warnings(self, guild_id: int, user_id: int) -> int:
        return await self.moderation_repository.clear_active_warnings(guild_id, user_id)

    async def add_audit(self, guild_id: int, actor_id: int, actor_name: str, action_type: str, target_id: int, metadata: dict):
        await self.guild_repository.add_audit_event(
            guild_id=str(guild_id),
            actor_id=str(actor_id),
            actor_name=str(actor_name),
            action_type=str(action_type),
            target_id=str(target_id),
            metadata=metadata or {},
            created_at=int(time.time()),
        )
