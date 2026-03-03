VERSION = "1.1.0"
import logging

import discord

LOGGER = logging.getLogger("discordbot.modules.welcome")

DEFAULT = {
    "enabled": False,
    "channel_id": "",
    "title": "Welcome to {server}, {user}",
    "description": (
        "You are now verified and ready to go.\n"
        "1) Read #rules\n"
        "2) Pick your roles in #get-roles\n"
        "3) Need help? Open a ticket in #open-ticket"
    ),
    "color": "#5865F2",
    "image": "",
    "footer": "If you are new, start with #rules and #get-roles.",
    "thumbnail": "",
}


def _color_to_int(value: str) -> int:
    try:
        return int(str(value).strip().replace("#", ""), 16)
    except Exception:
        return int("5865F2", 16)


def _render(text: str, member: discord.Member) -> str:
    return str(text).replace("{user}", member.mention).replace("{server}", member.guild.name)


async def on_member_join(bot: discord.Client, member: discord.Member, cfg: dict):
    if not cfg.get("enabled", False):
        return

    channel_id = str(cfg.get("channel_id", "")).strip()
    if not channel_id.isdigit():
        return

    channel = member.guild.get_channel(int(channel_id))
    if channel is None:
        return

    embed = discord.Embed(
        title=_render(cfg.get("title", DEFAULT["title"]), member),
        description=_render(cfg.get("description", DEFAULT["description"]), member),
        color=_color_to_int(cfg.get("color", DEFAULT["color"])),
    )

    image_url = str(cfg.get("image", "")).strip()
    if image_url:
        embed.set_image(url=image_url)

    thumb_url = str(cfg.get("thumbnail", "")).strip()
    if thumb_url:
        embed.set_thumbnail(url=thumb_url)

    footer_text = str(cfg.get("footer", "")).strip()
    if footer_text:
        embed.set_footer(text=_render(footer_text, member))

    try:
        await channel.send(embed=embed)
    except Exception:
        LOGGER.warning("welcome_send_failed guild_id=%s channel_id=%s", member.guild.id, channel.id, exc_info=True)
