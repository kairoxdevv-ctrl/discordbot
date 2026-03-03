"""Feature module: moderation/logging embeds sent to configured log channels."""

VERSION = "1.1.0"
import logging
from datetime import datetime, timezone

import discord

LOGGER = logging.getLogger("discordbot.module.logging")

DEFAULT = {
    "enabled": False,
    "channel_id": "",
    "joins": True,
    "leaves": True,
    "bans": True,
    "kicks": True,
    "message_deletions": True,
}


def _now():
    return datetime.now(timezone.utc)


async def send_log(guild: discord.Guild, cfg: dict, title: str, description: str):
    if not cfg.get("enabled", False):
        return

    channel_id = str(cfg.get("channel_id", "")).strip()
    if not channel_id.isdigit():
        return

    channel = guild.get_channel(int(channel_id))
    if channel is None:
        return

    embed = discord.Embed(title=title, description=description, color=0x2F3136, timestamp=_now())
    try:
        await channel.send(embed=embed)
    except Exception:
        LOGGER.warning("send_log_failed guild_id=%s channel_id=%s", guild.id, channel.id, exc_info=True)


async def on_member_join(member: discord.Member, cfg: dict):
    if cfg.get("joins", True):
        await send_log(member.guild, cfg, "Member Joined", f"{member.mention} (`{member.id}`) joined.")


async def on_member_remove(member: discord.Member, cfg: dict):
    if cfg.get("leaves", True):
        await send_log(member.guild, cfg, "Member Left", f"{member} (`{member.id}`) left.")


async def on_member_ban(guild: discord.Guild, user: discord.abc.User, cfg: dict):
    if cfg.get("bans", True):
        await send_log(guild, cfg, "Member Banned", f"{user} (`{user.id}`) was banned.")


async def on_message_delete(message: discord.Message, cfg: dict):
    if not message.guild or message.author.bot:
        return
    if cfg.get("message_deletions", True):
        content = (message.content or "").strip()
        if len(content) > 400:
            content = content[:400] + "..."
        await send_log(
            message.guild,
            cfg,
            "Message Deleted",
            f"Author: {message.author.mention}\nChannel: {message.channel.mention}\nContent: {content or '[no text]'}",
        )


async def detect_and_log_kick(member: discord.Member, cfg: dict):
    if not cfg.get("kicks", True):
        return
    me = member.guild.me
    if me is None or not member.guild.me.guild_permissions.view_audit_log:
        return

    try:
        async for entry in member.guild.audit_logs(limit=5, action=discord.AuditLogAction.kick):
            if entry.target.id == member.id and (_now() - entry.created_at).total_seconds() <= 15:
                moderator = entry.user
                await send_log(
                    member.guild,
                    cfg,
                    "Member Kicked",
                    f"{member} (`{member.id}`) was kicked by {moderator}.",
                )
                return
    except Exception:
        LOGGER.warning("detect_and_log_kick_failed guild_id=%s user_id=%s", member.guild.id, member.id, exc_info=True)
        return
