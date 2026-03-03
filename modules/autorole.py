"""Feature module: autorole assignment for new members."""

VERSION = "1.1.0"
import discord

DEFAULT = {
    "enabled": False,
    "role_id": "",
}


async def on_member_join(member: discord.Member, cfg: dict):
    if not cfg.get("enabled", False):
        return

    role_id = str(cfg.get("role_id", "")).strip()
    if not role_id.isdigit():
        return

    role = member.guild.get_role(int(role_id))
    if role is None:
        return

    if role >= member.guild.me.top_role:
        return

    await member.add_roles(role, reason="Autorole")
