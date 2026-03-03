"""Centralized permission and role hierarchy validation."""

from __future__ import annotations

import discord


class PermissionService:
    """Shared permission checks for bot commands and dashboard access."""

    def validate_role_hierarchy(self, actor: discord.Member, target: discord.Member) -> tuple[bool, str]:
        """Validate actor/target hierarchy constraints for moderation actions."""
        if actor.id == target.id:
            return False, "You cannot target yourself."
        if target.id == actor.guild.owner_id:
            return False, "You cannot target the server owner."
        me = actor.guild.me
        if me and target.id == me.id:
            return False, "You cannot target the bot."
        if actor.id != actor.guild.owner_id and target.top_role >= actor.top_role:
            return False, "Target has equal or higher role."
        if me and target.top_role >= me.top_role:
            return False, "Bot role is not high enough."
        return True, ""

    def require_permissions(self, member: discord.Member, permission_flags: list[str]) -> bool:
        """Return True only when all requested guild permission flags are present."""
        perms = member.guild_permissions
        for flag in permission_flags:
            if not bool(getattr(perms, str(flag), False)):
                return False
        return True

    def validate_moderation_action(
        self,
        actor: discord.Member,
        target: discord.Member,
        action_type: str,
        allowed_role_ids: set[str] | None = None,
    ) -> tuple[bool, str]:
        """Validate access + hierarchy for moderation actions."""
        if actor.id == actor.guild.owner_id:
            ok, err = self.validate_role_hierarchy(actor, target)
            return ok, err

        role_ids = allowed_role_ids or set()
        has_role = any(str(r.id) in role_ids for r in actor.roles) if role_ids else False
        has_perm = actor.guild_permissions.manage_guild or actor.guild_permissions.moderate_members
        if not (has_role or has_perm):
            return False, "Missing moderation access."

        return self.validate_role_hierarchy(actor, target)

    def validate_dashboard_access(self, manageable_ids: set[str], guild_id: str, is_owner: bool, is_support: bool) -> bool:
        """Validate whether the current user may manage dashboard data for a guild."""
        gid = str(guild_id)
        if gid in manageable_ids:
            return True
        return bool(is_owner or is_support)
