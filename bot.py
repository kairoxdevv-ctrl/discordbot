import asyncio
import io
import logging
import os
import re
import signal
import threading
import time
from datetime import timedelta
from pathlib import Path

import discord
from discord import app_commands
from dotenv import load_dotenv

from core.metrics import MetricsEngine
from core.module_registry import ModuleRegistry
from core.scheduler import TaskScheduler
from core.logging_setup import setup_structured_logging
from core.ws_server import start_ws_server
from repositories.config_repository import ConfigRepository
from repositories.guild_repository import GuildRepository
from repositories.moderation_repository import ModerationRepository
from services.abuse_service import AbuseService
from services.config_service import ConfigService
from services.custom_command_service import CustomCommandService
from services.guild_service import GuildService
from services.moderation_service import ModerationService
from services.permission_service import PermissionService
from services.realtime_service import RealtimeService
from dashboard import (
    BASE_DIR,
    CONFIG_ENGINE,
    REALTIME_BUS,
    REPO,
    RUNTIME,
    WS_AUTH,
    run_dashboard,
    set_module_health_provider,
    set_scheduler_provider,
)
from modules.custom_commands import CustomCommandManager

ENV_PATH = BASE_DIR / ".env"
load_dotenv(ENV_PATH)
setup_structured_logging(os.getenv("LOG_LEVEL", "INFO"))
TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN missing")

intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.guilds = True

bot = discord.AutoShardedClient(intents=intents)
tree = app_commands.CommandTree(bot)

MODULES_DIR = BASE_DIR / "modules"
REGISTRY = ModuleRegistry(MODULES_DIR)
METRICS = MetricsEngine(REPO)
SCHEDULER = TaskScheduler(REPO)
CUSTOM_COMMANDS = CustomCommandManager()
CUSTOM_COMMAND_SERVICE = CustomCommandService(CUSTOM_COMMANDS, cooldown_sec=max(1, int(os.getenv("CUSTOM_COMMAND_COOLDOWN_SEC", "3"))))
LOGGER = logging.getLogger("discordbot.bot")
MODULE_TIMEOUT_SEC = float(os.getenv("MODULE_TIMEOUT_SEC", "8"))
TREE_SYNC_TIMEOUT_SEC = float(os.getenv("TREE_SYNC_TIMEOUT_SEC", "90"))
try:
    CONFIGTEST_OWNER_ID = int(
        (os.getenv("CONFIGTEST_OWNER_ID", "") or os.getenv("OWNER_ID", "") or "1451546709422247987").strip()
    )
except Exception:
    CONFIGTEST_OWNER_ID = 1451546709422247987
_BACKGROUND_TASKS_STARTED = False
MODERATION_REPOSITORY = ModerationRepository(REPO.pool)
GUILD_REPOSITORY = GuildRepository(REPO.pool)
PERMISSION_SERVICE = PermissionService()
ABUSE_SERVICE = AbuseService(
    per_user_limit=max(3, int(os.getenv("COMMAND_RATE_LIMIT_PER_USER", "12"))),
    per_guild_limit=max(10, int(os.getenv("COMMAND_RATE_LIMIT_PER_GUILD", "80"))),
    window_sec=max(3, int(os.getenv("COMMAND_RATE_LIMIT_WINDOW_SEC", "30"))),
    burst_limit=max(2, int(os.getenv("COMMAND_BURST_LIMIT", "6"))),
    burst_window_sec=max(1, int(os.getenv("COMMAND_BURST_WINDOW_SEC", "5"))),
)
CONFIG_SERVICE = ConfigService(
    ConfigRepository(CONFIG_ENGINE),
    ttl_sec=max(5, int(os.getenv("CONFIG_CACHE_TTL_SEC", "30"))),
)
MODERATION_SERVICE = ModerationService(MODERATION_REPOSITORY, GUILD_REPOSITORY)
REALTIME_SERVICE = RealtimeService(REALTIME_BUS, WS_AUTH)
GUILD_SERVICE = GuildService(RUNTIME)
DASHBOARD_THREAD = None


async def _task_handler(guild_id: str, payload: dict):
    REALTIME_BUS.publish(guild_id, {"type": "scheduled_task", "payload": payload, "ts": int(time.time())})


def _attach_task_logging(task: asyncio.Task, name: str):
    def _done(t: asyncio.Task):
        try:
            t.result()
        except asyncio.CancelledError:
            LOGGER.info("background_task_cancelled name=%s", name)
        except Exception:
            LOGGER.exception("background_task_failed name=%s", name)

    task.add_done_callback(_done)
    return task


async def _run_with_timeout(coro, op: str, timeout_sec: float | None = None, **fields):
    try:
        return await asyncio.wait_for(coro, timeout=timeout_sec or MODULE_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        LOGGER.error("module_timeout op=%s fields=%s", op, fields)
    except Exception:
        LOGGER.warning("module_exec_failed op=%s fields=%s", op, fields, exc_info=True)
    return False


async def _realtime_stats_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        for guild in bot.guilds:
            online = len([m for m in guild.members if str(getattr(m, "status", "offline")) != "offline"])
            REALTIME_BUS.publish(
                str(guild.id),
                {
                    "type": "stats_update",
                    "member_count": guild.member_count or len(guild.members),
                    "online_count": online,
                    "boost_level": int(guild.premium_tier or 0),
                    "latency_ms": int((bot.latency or 0.0) * 1000),
                },
            )
        await asyncio.sleep(12)


def _publish_stats_snapshot(guild: discord.Guild):
    m_stats = module("stats")
    if m_stats and hasattr(m_stats, "build_stats"):
        try:
            stats = m_stats.build_stats(guild, RUNTIME.latency_ms())
            REALTIME_BUS.publish(str(guild.id), {"type": "stats_update", **stats})
            return
        except Exception:
            LOGGER.warning("stats_module_build_failed guild_id=%s", guild.id, exc_info=True)
    online = len([m for m in guild.members if str(getattr(m, "status", "offline")) != "offline"])
    REALTIME_BUS.publish(
        str(guild.id),
        {
            "type": "stats_update",
            "member_count": guild.member_count or len(guild.members),
            "online_count": online,
            "boost_level": int(guild.premium_tier or 0),
            "latency_ms": RUNTIME.latency_ms(),
        },
    )


def _on_config_reload():
    try:
        REGISTRY.hot_reload()
    except Exception:
        LOGGER.warning("registry_hot_reload_failed", exc_info=True)
    loop = getattr(bot, "loop", None)
    if not loop or loop.is_closed():
        return
    try:
        fut = asyncio.run_coroutine_threadsafe(
            _run_with_timeout(CUSTOM_COMMAND_SERVICE.sync(tree, CONFIG_ENGINE), "custom_commands_sync_on_reload", timeout_sec=30),
            loop,
        )
        fut.add_done_callback(lambda f: f.exception() and LOGGER.warning("custom_commands_reload_callback_failed", exc_info=True))
    except Exception:
        LOGGER.warning("config_reload_sync_schedule_failed", exc_info=True)


def guild_modules(guild_id: int):
    return CONFIG_SERVICE.get_guild_config(str(guild_id)).get("modules", {})


def module(name: str):
    return REGISTRY.get(name)


def reaction_roles_cfg(guild_id: int) -> dict:
    modules = guild_modules(guild_id)
    cfg = modules.get("reaction_roles", {})
    return cfg if isinstance(cfg, dict) else {}


def _emoji_candidates(emoji: discord.PartialEmoji) -> set[str]:
    out = set()
    s = str(emoji or "")
    if s:
        out.add(s)
    if getattr(emoji, "id", None):
        out.add(str(emoji.id))
    if getattr(emoji, "name", None):
        out.add(str(emoji.name))
    return out


def _reaction_role_target(cfg: dict, channel_id: int, message_id: int, emoji: discord.PartialEmoji) -> str:
    if not bool(cfg.get("enabled", False)):
        return ""
    target_channel_id = str(cfg.get("channel_id", ""))
    if target_channel_id and not target_channel_id.isdigit():
        return ""
    if target_channel_id and target_channel_id != str(channel_id):
        return ""
    if str(cfg.get("message_id", "")) != str(message_id):
        return ""
    candidates = _emoji_candidates(emoji)
    pairs = cfg.get("pairs", [])
    if not isinstance(pairs, list):
        return ""
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        em = str(pair.get("emoji", "")).strip()
        rid = str(pair.get("role_id", "")).strip()
        if not em or not rid.isdigit():
            continue
        if em in candidates:
            return rid
    return ""


def moderation_cfg(guild_id: int) -> dict:
    modules = guild_modules(guild_id)
    cfg = modules.get("moderation_tools", {})
    return cfg if isinstance(cfg, dict) else {}


def commands_cfg(guild_id: int) -> dict:
    modules = guild_modules(guild_id)
    cfg = modules.get("commands", {})
    return cfg if isinstance(cfg, dict) else {}


def command_enabled_for_guild(guild_id: int, key: str, default: bool = True) -> bool:
    cfg = commands_cfg(guild_id)
    if not cfg:
        return default
    if not bool(cfg.get("enabled", True)):
        return False
    return bool(cfg.get(key, default))


def moderation_command_enabled(guild_id: int, cfg: dict, key: str) -> bool:
    if not bool(cfg.get("enabled", False)):
        return False
    own_flag = bool(cfg.get(key, False))
    return command_enabled_for_guild(guild_id, key, default=own_flag)


def has_moderation_access(interaction: discord.Interaction, cfg: dict) -> bool:
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return False
    if interaction.user.id == interaction.guild.owner_id:
        return True
    if PERMISSION_SERVICE.require_permissions(interaction.user, ["manage_guild"]) or PERMISSION_SERVICE.require_permissions(
        interaction.user, ["moderate_members"]
    ):
        return True
    allowed_roles = {str(x) for x in cfg.get("moderator_role_ids", []) if str(x).isdigit()}
    if not allowed_roles:
        return False
    return any(str(r.id) in allowed_roles for r in interaction.user.roles)


async def enforce_command_rate_limit(interaction: discord.Interaction, command_name: str) -> bool:
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return False
    allowed, retry_after = await ABUSE_SERVICE.allow(interaction.guild.id, interaction.user.id, command_name)
    if allowed:
        return True
    if not interaction.response.is_done():
        await interaction.response.send_message(
            f"Rate limited. Try again in {retry_after}s.",
            ephemeral=True,
        )
    return False


def _ticket_owner_id_from_topic(topic: str) -> int:
    m = re.search(r"ticket_owner:(\d+)", str(topic or ""))
    return int(m.group(1)) if m else 0


def _ticket_staff_roles(guild: discord.Guild) -> list[discord.Role]:
    cfg = moderation_cfg(guild.id)
    role_ids = {str(x) for x in cfg.get("moderator_role_ids", []) if str(x).isdigit()}
    roles: list[discord.Role] = []
    for rid in role_ids:
        role = guild.get_role(int(rid))
        if role:
            roles.append(role)
    if roles:
        return roles
    for name in ("Support Team", "Ticket Manager"):
        role = discord.utils.get(guild.roles, name=name)
        if role:
            roles.append(role)
    return roles


def _can_close_ticket(member: discord.Member, guild: discord.Guild) -> bool:
    if member.id == guild.owner_id:
        return True
    if member.guild_permissions.administrator:
        return True
    staff_role_ids = {r.id for r in _ticket_staff_roles(guild)}
    if not staff_role_ids:
        return False
    return any(r.id in staff_role_ids for r in member.roles)


def _find_open_ticket_channel(guild: discord.Guild, user_id: int) -> discord.TextChannel | None:
    marker = f"ticket_owner:{int(user_id)}"
    for ch in guild.text_channels:
        if not ch.name.startswith("ticket-"):
            continue
        if marker in str(ch.topic or ""):
            return ch
    return None


async def _open_ticket_for_member(guild: discord.Guild, member: discord.Member, reason: str) -> tuple[discord.TextChannel | None, str]:
    existing = _find_open_ticket_channel(guild, member.id)
    if existing is not None:
        return existing, "existing"

    me = guild.me
    if me is None or not me.guild_permissions.manage_channels:
        return None, "missing_permissions"

    category = discord.utils.get(guild.categories, name="TICKETS") or discord.utils.get(guild.categories, name="tickets")
    if category is None:
        category = await guild.create_category("TICKETS", reason="ticket system auto setup")

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        member: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
        ),
        me: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_channels=True,
            manage_messages=True,
        ),
    }
    for role in _ticket_staff_roles(guild):
        overwrites[role] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_messages=True,
        )

    ticket_name = f"ticket-{member.id}"
    channel = await guild.create_text_channel(
        ticket_name[:95],
        category=category,
        overwrites=overwrites,
        topic=f"ticket_owner:{member.id} opener:{member.id} created_at:{int(time.time())}",
        reason=f"ticket opened by {member}",
    )
    embed = discord.Embed(
        title="Support Ticket Opened",
        description=(
            f"{member.mention}, your private ticket is now open.\n\n"
            f"Reason: {reason[:500]}\n\n"
            "Use the button below to close this ticket when your issue is resolved."
        ),
        color=discord.Colour.blue(),
    )
    view = discord.ui.View(timeout=None)
    view.add_item(
        discord.ui.Button(
            label="Close Ticket",
            style=discord.ButtonStyle.danger,
            custom_id="ticket:close",
        )
    )
    await channel.send(embed=embed, view=view)
    await _send_ticket_log(
        guild,
        title="Ticket Opened",
        description=f"Channel: {channel.mention}\nOpened by: {member.mention}\nReason: {reason[:300]}",
        color=discord.Colour.green(),
    )
    return channel, "created"


def _ticket_log_channel(guild: discord.Guild) -> discord.TextChannel | None:
    return (
        discord.utils.get(guild.text_channels, name="ticket-logs")
        or discord.utils.get(guild.text_channels, name="ticket_logs")
        or discord.utils.get(guild.text_channels, name="mod-logs")
    )


def _ticket_transcripts_channel(guild: discord.Guild) -> discord.TextChannel | None:
    return (
        discord.utils.get(guild.text_channels, name="ticket-transcripts")
        or discord.utils.get(guild.text_channels, name="ticket_transcripts")
    )


async def _send_ticket_log(guild: discord.Guild, title: str, description: str, color: discord.Colour):
    ch = _ticket_log_channel(guild)
    if ch is None:
        return
    try:
        embed = discord.Embed(title=title[:120], description=description[:3500], color=color, timestamp=discord.utils.utcnow())
        await ch.send(embed=embed)
    except Exception:
        LOGGER.warning("ticket_log_send_failed guild_id=%s", guild.id, exc_info=True)


async def _build_ticket_transcript(channel: discord.TextChannel) -> tuple[bytes, int]:
    lines = []
    count = 0
    async for msg in channel.history(limit=5000, oldest_first=True):
        count += 1
        ts = msg.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
        author = f"{msg.author} ({msg.author.id})"
        content = (msg.content or "").strip()
        lines.append(f"[{ts}] {author}: {content}")
        if msg.attachments:
            lines.append("  attachments: " + ", ".join(str(a.url) for a in msg.attachments))
        if msg.embeds:
            lines.append(f"  embeds: {len(msg.embeds)}")
    if not lines:
        lines = ["No messages found in this ticket."]
    data = ("\n".join(lines) + "\n").encode("utf-8", errors="replace")
    return data, count


async def _archive_ticket_and_delete(channel: discord.TextChannel, actor: discord.Member, reason: str):
    guild = channel.guild
    owner_id = _ticket_owner_id_from_topic(channel.topic or "")
    transcript_message = None
    trans_ch = _ticket_transcripts_channel(guild)
    if trans_ch is not None:
        try:
            data, msg_count = await _build_ticket_transcript(channel)
            filename = f"{channel.name}-{int(time.time())}.txt"
            file = discord.File(fp=io.BytesIO(data), filename=filename)
            embed = discord.Embed(
                title="Ticket Transcript",
                description=(
                    f"Ticket: #{channel.name}\n"
                    f"Closed by: {actor.mention}\n"
                    f"Ticket owner: <@{owner_id}>"
                    if owner_id
                    else f"Ticket: #{channel.name}\nClosed by: {actor.mention}"
                ),
                color=discord.Colour.blurple(),
                timestamp=discord.utils.utcnow(),
            )
            embed.add_field(name="Reason", value=(reason or "Resolved")[:500], inline=False)
            embed.add_field(name="Message Count", value=str(msg_count), inline=True)
            transcript_message = await trans_ch.send(embed=embed, file=file)
        except Exception:
            LOGGER.warning("ticket_transcript_archive_failed guild_id=%s channel_id=%s", guild.id, channel.id, exc_info=True)

    log_desc = (
        f"Ticket: #{channel.name} (`{channel.id}`)\n"
        f"Closed by: {actor.mention}\n"
        f"Reason: {(reason or 'Resolved')[:300]}"
    )
    if owner_id:
        log_desc += f"\nOwner: <@{owner_id}>"
    if transcript_message is not None:
        log_desc += f"\nTranscript: {transcript_message.jump_url}"
    await _send_ticket_log(guild, "Ticket Closed", log_desc, discord.Colour.red())
    await channel.delete(reason=f"ticket closed by {actor} ({(reason or 'Resolved')[:200]})")


async def add_warning(guild_id: int, user_id: int, moderator_id: int, reason: str):
    await MODERATION_SERVICE.add_warning(guild_id, user_id, moderator_id, reason)


async def count_active_warnings(guild_id: int, user_id: int) -> int:
    return await MODERATION_SERVICE.count_active_warnings(guild_id, user_id)


async def clear_active_warnings(guild_id: int, user_id: int) -> int:
    return await MODERATION_SERVICE.clear_active_warnings(guild_id, user_id)


async def add_audit(guild_id: int, actor: discord.Member, action_type: str, target_id: int = 0, metadata: dict | None = None):
    await MODERATION_SERVICE.add_audit(
        guild_id=guild_id,
        actor_id=actor.id,
        actor_name=str(actor),
        action_type=action_type,
        target_id=target_id,
        metadata=metadata or {},
    )


async def _run_configtest(message: discord.Message):
    guild = message.guild
    if guild is None:
        return

    me = guild.me
    if me is None or not me.guild_permissions.manage_channels or not me.guild_permissions.manage_roles:
        await message.channel.send("configtest failed: bot needs Manage Channels + Manage Roles.", delete_after=12)
        return

    created = {"roles": 0, "channels": 0, "categories": 0}
    updated = {"roles": 0, "channels": 0, "categories": 0}

    def find_role(name: str):
        for r in guild.roles:
            if r.name == name:
                return r
        return None

    async def ensure_role(name: str, color: int, permissions: discord.Permissions, hoist: bool = False, mentionable: bool = False):
        role = find_role(name)
        if role:
            edits = {}
            if role.colour.value != color:
                edits["colour"] = discord.Colour(color)
            if role.permissions.value != permissions.value:
                edits["permissions"] = permissions
            if role.hoist != hoist:
                edits["hoist"] = hoist
            if role.mentionable != mentionable:
                edits["mentionable"] = mentionable
            if edits:
                await role.edit(reason="dashboard config test setup", **edits)
                updated["roles"] += 1
            return role
        role = await guild.create_role(
            name=name,
            colour=discord.Colour(color),
            permissions=permissions,
            hoist=hoist,
            mentionable=mentionable,
            reason="dashboard config test setup",
        )
        created["roles"] += 1
        return role

    async def ensure_category(name: str, overwrites: dict):
        category = discord.utils.get(guild.categories, name=name)
        if category:
            await category.edit(overwrites=overwrites, reason="dashboard config test setup")
            updated["categories"] += 1
            return category
        category = await guild.create_category(name, overwrites=overwrites, reason="dashboard config test setup")
        created["categories"] += 1
        return category

    async def ensure_channel(name: str, category: discord.CategoryChannel, overwrites: dict, topic: str = ""):
        ch = discord.utils.get(guild.text_channels, name=name)
        if ch:
            await ch.edit(category=category, overwrites=overwrites, topic=topic[:1024], reason="dashboard config test setup")
            updated["channels"] += 1
            return ch
        ch = await guild.create_text_channel(
            name,
            category=category,
            overwrites=overwrites,
            topic=topic[:1024],
            reason="dashboard config test setup",
        )
        created["channels"] += 1
        return ch

    role_member = await ensure_role("Member", 0x4B8DFF, discord.Permissions.none(), hoist=True)
    role_support = await ensure_role(
        "Support Team",
        0x57F287,
        discord.Permissions(
            view_audit_log=True,
            kick_members=True,
            moderate_members=True,
            manage_messages=True,
            read_message_history=True,
            send_messages=True,
            attach_files=True,
            embed_links=True,
        ),
        hoist=True,
        mentionable=True,
    )
    role_ticket = await ensure_role(
        "Ticket Manager",
        0xFEE75C,
        discord.Permissions(
            manage_channels=True,
            manage_messages=True,
            read_message_history=True,
            send_messages=True,
            attach_files=True,
            embed_links=True,
        ),
        hoist=True,
        mentionable=True,
    )
    role_muted = await ensure_role("Muted", 0x2F3136, discord.Permissions.none())
    role_ann = await ensure_role("Announcements", 0x5865F2, discord.Permissions.none(), mentionable=True)
    role_events = await ensure_role("Events", 0xEB459E, discord.Permissions.none(), mentionable=True)

    muted_deny = discord.PermissionOverwrite(send_messages=False, add_reactions=False, speak=False, connect=False)
    bot_allow = discord.PermissionOverwrite(
        view_channel=True,
        send_messages=True,
        read_message_history=True,
        manage_channels=True,
        manage_messages=True,
    )
    support_allow = discord.PermissionOverwrite(
        view_channel=True,
        send_messages=True,
        read_message_history=True,
        manage_messages=True,
    )
    ticket_allow = discord.PermissionOverwrite(
        view_channel=True,
        send_messages=True,
        read_message_history=True,
        manage_messages=True,
        manage_channels=True,
    )

    ow_public = {
        guild.default_role: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
        role_muted: muted_deny,
        me: bot_allow,
    }
    ow_readonly = {
        guild.default_role: discord.PermissionOverwrite(view_channel=True, send_messages=False, read_message_history=True),
        role_support: support_allow,
        role_ticket: ticket_allow,
        role_muted: muted_deny,
        me: bot_allow,
    }
    ow_staff = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        role_support: support_allow,
        role_ticket: ticket_allow,
        role_muted: muted_deny,
        me: bot_allow,
    }

    cat_info = await ensure_category("START HERE", ow_public)
    cat_support = await ensure_category("SUPPORT", ow_public)
    cat_tickets = await ensure_category("TICKETS", ow_staff)
    cat_staff = await ensure_category("STAFF", ow_staff)

    ch_rules = await ensure_channel("rules", cat_info, ow_readonly, "Server rules and policies.")
    ch_ann = await ensure_channel("announcements", cat_info, ow_readonly, "Official updates and announcements.")
    ch_welcome = await ensure_channel("welcome", cat_info, ow_public, "Welcome messages for new members.")
    ch_roles = await ensure_channel("get-roles", cat_info, ow_public, "Reaction roles and notifications.")
    ch_support = await ensure_channel("support-chat", cat_support, ow_public, "Community support and troubleshooting.")
    ch_open_ticket = await ensure_channel("open-ticket", cat_support, ow_public, "Instructions for opening support tickets.")
    ch_ticket_logs = await ensure_channel("ticket-logs", cat_tickets, ow_staff, "Ticket activity and system actions.")
    ch_ticket_transcripts = await ensure_channel("ticket-transcripts", cat_tickets, ow_staff, "Archived ticket transcripts.")
    ch_staff_chat = await ensure_channel("staff-chat", cat_staff, ow_staff, "Private staff coordination.")
    ch_mod_logs = await ensure_channel("mod-logs", cat_staff, ow_staff, "Moderation and member action logs.")
    ch_bot_commands = await ensure_channel("bot-commands", cat_staff, ow_staff, "Bot/admin command channel.")

    cfg = CONFIG_SERVICE.get_guild_config(str(guild.id))
    modules_cfg = cfg.setdefault("modules", {})

    welcome_cfg = modules_cfg.setdefault("welcome", {})
    welcome_cfg["enabled"] = True
    welcome_cfg["channel_id"] = str(ch_welcome.id)
    welcome_cfg["title"] = "Welcome to {server}, {user}"
    welcome_cfg["description"] = (
        "Start here:\n"
        "1) Read #rules\n"
        "2) Pick your roles in #get-roles\n"
        "3) Need help? Open a ticket in #open-ticket"
    )
    welcome_cfg["footer"] = "If you are new, this channel shows key updates and guidance."

    logging_cfg = modules_cfg.setdefault("logging", {})
    logging_cfg["enabled"] = True
    logging_cfg["channel_id"] = str(ch_mod_logs.id)
    logging_cfg["joins"] = True
    logging_cfg["leaves"] = True
    logging_cfg["bans"] = True
    logging_cfg["kicks"] = True
    logging_cfg["message_deletions"] = True

    autorole_cfg = modules_cfg.setdefault("autorole", {})
    autorole_cfg["enabled"] = True
    autorole_cfg["role_id"] = str(role_member.id)

    moderation_cfg_local = modules_cfg.setdefault("moderation_tools", {})
    moderation_cfg_local["enabled"] = True
    moderation_cfg_local["moderator_role_ids"] = [str(role_support.id), str(role_ticket.id)]
    moderation_cfg_local["command_warn"] = True
    moderation_cfg_local["command_warnings"] = True
    moderation_cfg_local["command_clearwarnings"] = True
    moderation_cfg_local["command_timeout"] = True
    moderation_cfg_local["command_kick"] = True
    moderation_cfg_local["command_ban"] = True

    commands_cfg_local = modules_cfg.setdefault("commands", {})
    commands_cfg_local["enabled"] = True
    commands_cfg_local["command_warn"] = True
    commands_cfg_local["command_warnings"] = True
    commands_cfg_local["command_clearwarnings"] = True
    commands_cfg_local["command_timeout"] = True
    commands_cfg_local["command_kick"] = True
    commands_cfg_local["command_ban"] = True

    reaction_cfg = modules_cfg.setdefault("reaction_roles", {})
    reaction_cfg["enabled"] = True
    reaction_cfg["channel_id"] = str(ch_roles.id)
    reaction_cfg["message_id"] = str(reaction_cfg.get("message_id", "") or "")
    reaction_cfg["message_template"] = "React below to get your roles.\n{roles}"
    reaction_cfg["show_role_list"] = True
    reaction_cfg["pairs"] = [
        {"emoji": "📢", "role_id": str(role_ann.id)},
        {"emoji": "🎉", "role_id": str(role_events.id)},
    ]

    automod_cfg = modules_cfg.setdefault("automod", {})
    automod_cfg["enabled"] = True
    automod_cfg["log_violations"] = True
    automod_cfg["exempt_channel_ids"] = [str(ch_staff_chat.id), str(ch_bot_commands.id)]
    automod_cfg["exempt_role_ids"] = [str(role_support.id), str(role_ticket.id)]

    settings_cfg = modules_cfg.setdefault("settings", {})
    settings_cfg["enabled"] = True
    settings_cfg["verify_enabled"] = True
    settings_cfg["lock_enabled"] = False
    settings_cfg["verified_role_id"] = str(role_member.id)
    settings_cfg["verify_channel_id"] = str(ch_open_ticket.id)
    settings_cfg["lock_mode"] = str(settings_cfg.get("lock_mode", "all_except_verify") or "all_except_verify")
    settings_cfg["verify_lock_category_ids"] = settings_cfg.get("verify_lock_category_ids", [])
    settings_cfg["verify_lock_last_applied_channel_ids"] = settings_cfg.get("verify_lock_last_applied_channel_ids", [])
    settings_cfg["verify_role_id"] = str(role_member.id)
    settings_cfg["verify_min_account_age_days"] = int(settings_cfg.get("verify_min_account_age_days", 7) or 7)
    settings_cfg["verify_panel_channel_id"] = str(ch_open_ticket.id)
    settings_cfg["verify_panel_title"] = "Server Verification"
    settings_cfg["verify_panel_body"] = "Click the button below to verify and unlock full access."
    settings_cfg["verify_visible_channel_ids"] = [
        str(ch_rules.id),
        str(ch_welcome.id),
        str(ch_roles.id),
        str(ch_support.id),
        str(ch_open_ticket.id),
    ]

    CONFIG_SERVICE.update_guild_config(str(guild.id), "welcome", welcome_cfg)
    CONFIG_SERVICE.update_guild_config(str(guild.id), "logging", logging_cfg)
    CONFIG_SERVICE.update_guild_config(str(guild.id), "autorole", autorole_cfg)
    CONFIG_SERVICE.update_guild_config(str(guild.id), "moderation_tools", moderation_cfg_local)
    CONFIG_SERVICE.update_guild_config(str(guild.id), "commands", commands_cfg_local)
    CONFIG_SERVICE.update_guild_config(str(guild.id), "reaction_roles", reaction_cfg)
    CONFIG_SERVICE.update_guild_config(str(guild.id), "automod", automod_cfg)
    CONFIG_SERVICE.update_guild_config(str(guild.id), "settings", settings_cfg)

    await message.channel.send(
        (
            "configtest ok\n"
            f"categories: {cat_info.name}, {cat_support.name}, {cat_tickets.name}, {cat_staff.name}\n"
            f"core channels: {ch_rules.mention}, {ch_ann.mention}, {ch_welcome.mention}, {ch_support.mention}, {ch_open_ticket.mention}\n"
            f"staff channels: {ch_ticket_logs.mention}, {ch_ticket_transcripts.mention}, {ch_staff_chat.mention}, {ch_mod_logs.mention}, {ch_bot_commands.mention}\n"
            f"roles: {role_member.mention}, {role_support.mention}, {role_ticket.mention}, {role_muted.mention}, {role_ann.mention}, {role_events.mention}\n"
            f"created -> categories:{created['categories']} channels:{created['channels']} roles:{created['roles']}\n"
            f"updated -> categories:{updated['categories']} channels:{updated['channels']} roles:{updated['roles']}"
        ),
        delete_after=30,
    )


def _validate_startup_dependencies():
    if not MODULES_DIR.exists():
        raise RuntimeError(f"modules_dir_missing path={MODULES_DIR}")
    _ = CONFIG_ENGINE.get_all()
    health = REGISTRY.discover()
    REGISTRY.resolve()
    required = ("welcome", "autorole", "automod", "logging", "stats")
    missing = [name for name in required if name not in health]
    if missing:
        raise RuntimeError(f"required_modules_missing={','.join(missing)}")
    if not hasattr(WS_AUTH, "issue"):
        raise RuntimeError("ws_auth_invalid")
    LOGGER.info("startup_dependencies_validated")


@bot.event
async def on_ready():
    global _BACKGROUND_TASKS_STARTED
    RUNTIME.set_bot(bot)
    REGISTRY.discover()
    REGISTRY.resolve()
    LOGGER.info("on_ready_start guilds=%s", len(bot.guilds))
    if not _BACKGROUND_TASKS_STARTED:
        _BACKGROUND_TASKS_STARTED = True
        _attach_task_logging(
            asyncio.create_task(_run_with_timeout(tree.sync(), "tree.sync", timeout_sec=TREE_SYNC_TIMEOUT_SEC)),
            "tree_sync",
        )
        _attach_task_logging(
            asyncio.create_task(
                _run_with_timeout(
                    CUSTOM_COMMAND_SERVICE.sync(tree, CONFIG_ENGINE),
                    "custom_commands_sync",
                    timeout_sec=TREE_SYNC_TIMEOUT_SEC,
                )
            ),
            "custom_commands_sync",
        )
        _attach_task_logging(asyncio.create_task(_realtime_stats_loop()), "realtime_stats_loop")
        _attach_task_logging(asyncio.create_task(SCHEDULER.run(_task_handler)), "scheduler_run")
    else:
        LOGGER.info("on_ready_background_already_started")
    LOGGER.info("on_ready_completed")


@bot.event
async def on_disconnect():
    RUNTIME.gateway_reconnects += 1
    RUNTIME.last_disconnect_at = int(time.time())


@bot.event
async def on_error(event_method, *args, **kwargs):
    LOGGER.exception("discord_global_error event=%s args=%s kwargs=%s", event_method, len(args), len(kwargs))


@bot.event
async def on_shard_ready(shard_id):
    RUNTIME.shard_ready[str(shard_id)] = int(time.time())


@bot.event
async def on_member_join(member: discord.Member):
    modules = guild_modules(member.guild.id)

    m_welcome = module("welcome")
    if m_welcome and hasattr(m_welcome, "on_member_join"):
        await _run_with_timeout(
            m_welcome.on_member_join(bot, member, modules.get("welcome", {})),
            "welcome.on_member_join",
            guild_id=member.guild.id,
            user_id=member.id,
        )

    m_autorole = module("autorole")
    if m_autorole and hasattr(m_autorole, "on_member_join"):
        await _run_with_timeout(
            m_autorole.on_member_join(member, modules.get("autorole", {})),
            "autorole.on_member_join",
            guild_id=member.guild.id,
            user_id=member.id,
        )

    m_log = module("logging")
    if m_log and hasattr(m_log, "on_member_join"):
        await _run_with_timeout(
            m_log.on_member_join(member, modules.get("logging", {})),
            "logging.on_member_join",
            guild_id=member.guild.id,
            user_id=member.id,
        )

    await METRICS.track(str(member.guild.id), "join", {"user_id": member.id})
    REALTIME_BUS.publish(str(member.guild.id), {"type": "join", "user_id": member.id})
    _publish_stats_snapshot(member.guild)


@bot.event
async def on_member_remove(member: discord.Member):
    modules = guild_modules(member.guild.id)
    m_log = module("logging")
    if m_log and hasattr(m_log, "on_member_remove"):
        await _run_with_timeout(
            m_log.on_member_remove(member, modules.get("logging", {})),
            "logging.on_member_remove",
            guild_id=member.guild.id,
            user_id=member.id,
        )
    if m_log and hasattr(m_log, "detect_and_log_kick"):
        await _run_with_timeout(
            m_log.detect_and_log_kick(member, modules.get("logging", {})),
            "logging.detect_and_log_kick",
            guild_id=member.guild.id,
            user_id=member.id,
        )
    await METRICS.track(str(member.guild.id), "leave", {"user_id": member.id})
    REALTIME_BUS.publish(str(member.guild.id), {"type": "leave", "user_id": member.id})
    _publish_stats_snapshot(member.guild)


@bot.event
async def on_member_ban(guild: discord.Guild, user: discord.abc.User):
    modules = guild_modules(guild.id)
    m_log = module("logging")
    if m_log and hasattr(m_log, "on_member_ban"):
        await _run_with_timeout(
            m_log.on_member_ban(guild, user, modules.get("logging", {})),
            "logging.on_member_ban",
            guild_id=guild.id,
            user_id=user.id,
        )
    await METRICS.track(str(guild.id), "ban", {"user_id": user.id})
    REALTIME_BUS.publish(str(guild.id), {"type": "ban", "user_id": user.id})
    _publish_stats_snapshot(guild)


@bot.event
async def on_message_delete(message: discord.Message):
    if not message.guild:
        return
    modules = guild_modules(message.guild.id)
    m_log = module("logging")
    if m_log and hasattr(m_log, "on_message_delete"):
        await _run_with_timeout(
            m_log.on_message_delete(message, modules.get("logging", {})),
            "logging.on_message_delete",
            guild_id=message.guild.id,
            channel_id=message.channel.id if message.channel else 0,
            message_id=message.id,
        )


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return

    content = (message.content or "").strip()
    if content.lower() == ".configtest" and int(message.author.id) == CONFIGTEST_OWNER_ID:
        await _run_with_timeout(
            _run_configtest(message),
            "configtest",
            guild_id=message.guild.id,
            channel_id=message.channel.id if message.channel else 0,
            user_id=message.author.id,
        )
        return

    modules = guild_modules(message.guild.id)
    automod_triggered = False
    m_automod = module("automod")
    if m_automod and hasattr(m_automod, "on_message"):
        automod_triggered = await _run_with_timeout(
            m_automod.on_message(message, modules.get("automod", {}), modules.get("logging", {})),
            "automod.on_message",
            guild_id=message.guild.id,
            channel_id=message.channel.id if message.channel else 0,
            user_id=message.author.id,
        )
    await METRICS.track(str(message.guild.id), "message", {"user_id": message.author.id})
    if automod_triggered:
        REALTIME_BUS.publish(str(message.guild.id), {"type": "automod_trigger", "ts": int(time.time())})


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    bot_user_id = getattr(getattr(bot, "user", None), "id", None)
    if payload.guild_id is None or (bot_user_id is not None and payload.user_id == bot_user_id):
        return
    cfg = reaction_roles_cfg(payload.guild_id)
    role_id = _reaction_role_target(cfg, payload.channel_id, payload.message_id, payload.emoji)
    if not role_id:
        return
    guild = bot.get_guild(payload.guild_id)
    if guild is None:
        return
    role = guild.get_role(int(role_id))
    if role is None:
        return
    member = payload.member
    if member is None:
        try:
            member = await guild.fetch_member(payload.user_id)
        except Exception:
            LOGGER.warning("reaction_role_fetch_member_failed guild_id=%s user_id=%s", payload.guild_id, payload.user_id, exc_info=True)
            return
    try:
        await member.add_roles(role, reason="Reaction role add")
        REALTIME_BUS.publish(str(payload.guild_id), {"type": "reaction_role_add", "user_id": payload.user_id, "role_id": int(role_id)})
    except Exception:
        LOGGER.warning(
            "reaction_role_add_failed guild_id=%s user_id=%s role_id=%s",
            payload.guild_id,
            payload.user_id,
            role_id,
            exc_info=True,
        )


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    bot_user_id = getattr(getattr(bot, "user", None), "id", None)
    if payload.guild_id is None or (bot_user_id is not None and payload.user_id == bot_user_id):
        return
    cfg = reaction_roles_cfg(payload.guild_id)
    role_id = _reaction_role_target(cfg, payload.channel_id, payload.message_id, payload.emoji)
    if not role_id:
        return
    guild = bot.get_guild(payload.guild_id)
    if guild is None:
        return
    role = guild.get_role(int(role_id))
    if role is None:
        return
    try:
        member = guild.get_member(payload.user_id) or await guild.fetch_member(payload.user_id)
    except Exception:
        LOGGER.warning("reaction_role_fetch_member_failed guild_id=%s user_id=%s", payload.guild_id, payload.user_id, exc_info=True)
        return
    try:
        await member.remove_roles(role, reason="Reaction role remove")
        REALTIME_BUS.publish(str(payload.guild_id), {"type": "reaction_role_remove", "user_id": payload.user_id, "role_id": int(role_id)})
    except Exception:
        LOGGER.warning(
            "reaction_role_remove_failed guild_id=%s user_id=%s role_id=%s",
            payload.guild_id,
            payload.user_id,
            role_id,
            exc_info=True,
        )


@bot.event
async def on_interaction(interaction: discord.Interaction):
    if interaction.type != discord.InteractionType.component:
        return
    data = interaction.data or {}
    custom_id = str(data.get("custom_id", ""))
    if custom_id not in {"ticket:open", "ticket:close"}:
        return
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        if not interaction.response.is_done():
            await interaction.response.send_message("Guild-only action.", ephemeral=True)
        return

    if custom_id == "ticket:open":
        if not await enforce_command_rate_limit(interaction, "ticket_open_button"):
            return
        if not command_enabled_for_guild(interaction.guild.id, "command_ticket_open", default=True):
            if not interaction.response.is_done():
                await interaction.response.send_message("Ticket button is disabled on this server.", ephemeral=True)
            return
        channel, status = await _open_ticket_for_member(interaction.guild, interaction.user, "Opened via ticket panel button")
        if status == "missing_permissions":
            if not interaction.response.is_done():
                await interaction.response.send_message("Bot is missing Manage Channels permission.", ephemeral=True)
            return
        if channel is None:
            if not interaction.response.is_done():
                await interaction.response.send_message("Could not open ticket.", ephemeral=True)
            return
        if status == "existing":
            if not interaction.response.is_done():
                await interaction.response.send_message(f"You already have an open ticket: {channel.mention}", ephemeral=True)
            return
        if not interaction.response.is_done():
            await interaction.response.send_message(f"Ticket opened: {channel.mention}", ephemeral=True)
        await METRICS.track(str(interaction.guild.id), "ticket_open", {"user_id": interaction.user.id, "channel_id": channel.id})
        await add_audit(
            interaction.guild.id,
            interaction.user,
            "ticket_open",
            target_id=channel.id,
            metadata={"source": "button"},
        )
        REALTIME_BUS.publish(str(interaction.guild.id), {"type": "ticket_open", "user_id": interaction.user.id, "channel_id": channel.id})
        return

    if not isinstance(interaction.channel, discord.TextChannel) or not interaction.channel.name.startswith("ticket-"):
        if not interaction.response.is_done():
            await interaction.response.send_message("This button only works inside a ticket channel.", ephemeral=True)
        return
    if not command_enabled_for_guild(interaction.guild.id, "command_ticket_close", default=True):
        if not interaction.response.is_done():
            await interaction.response.send_message("Ticket close button is disabled on this server.", ephemeral=True)
        return
    if not await enforce_command_rate_limit(interaction, "ticket_close_button"):
        return

    if not _can_close_ticket(interaction.user, interaction.guild):
        if not interaction.response.is_done():
            await interaction.response.send_message("Only admins or ticket staff can close this ticket.", ephemeral=True)
        return
    owner_id = _ticket_owner_id_from_topic(interaction.channel.topic or "")
    ch = interaction.channel
    if not interaction.response.is_done():
        await interaction.response.send_message("Closing ticket in 3 seconds...", ephemeral=True)
    await METRICS.track(
        str(interaction.guild.id),
        "ticket_close",
        {"user_id": interaction.user.id, "channel_id": ch.id, "ticket_owner_id": owner_id or 0},
    )
    await add_audit(
        interaction.guild.id,
        interaction.user,
        "ticket_close",
        target_id=ch.id,
        metadata={"source": "button", "ticket_owner_id": int(owner_id or 0)},
    )
    REALTIME_BUS.publish(str(interaction.guild.id), {"type": "ticket_close", "user_id": interaction.user.id, "channel_id": ch.id})
    await asyncio.sleep(3)
    await _archive_ticket_and_delete(ch, interaction.user, "Closed using ticket button")


@tree.command(name="warn", description="Warn a member")
@app_commands.describe(member="Member to warn", reason="Reason for warning")
async def warn(interaction: discord.Interaction, member: discord.Member, reason: str = "No reason provided"):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("Guild-only command.", ephemeral=True)
        return
    cfg = moderation_cfg(interaction.guild.id)
    if not moderation_command_enabled(interaction.guild.id, cfg, "command_warn"):
        await interaction.response.send_message("This command is disabled on this server.", ephemeral=True)
        return
    if not await enforce_command_rate_limit(interaction, "warn"):
        return
    ok, err = PERMISSION_SERVICE.validate_moderation_action(
        interaction.user,
        member,
        "warn",
        {str(x) for x in cfg.get("moderator_role_ids", []) if str(x).isdigit()},
    )
    if not ok:
        await interaction.response.send_message(err, ephemeral=True)
        return

    await add_warning(interaction.guild.id, member.id, interaction.user.id, reason)
    count = await count_active_warnings(interaction.guild.id, member.id)
    await interaction.response.send_message(f"Warned {member.mention}. Active warnings: {count}", ephemeral=False)
    await add_audit(
        interaction.guild.id,
        interaction.user,
        "moderation_warn",
        target_id=member.id,
        metadata={"reason": str(reason)[:300], "active_warnings": int(count)},
    )
    await METRICS.track(str(interaction.guild.id), "warn", {"user_id": member.id, "moderator_id": interaction.user.id})
    REALTIME_BUS.publish(str(interaction.guild.id), {"type": "warn", "user_id": member.id, "moderator_id": interaction.user.id})

    threshold = max(1, int(cfg.get("warn_threshold", 3)))
    if count < threshold:
        return
    if bool(cfg.get("auto_kick", False)):
        try:
            await member.kick(reason=f"Warn threshold reached ({count})")
            await interaction.followup.send(f"Auto action: kicked {member.mention} (threshold reached).", ephemeral=True)
            await add_audit(
                interaction.guild.id,
                interaction.user,
                "moderation_auto_kick",
                target_id=member.id,
                metadata={"warn_count": int(count)},
            )
        except Exception:
            LOGGER.warning("moderation_auto_kick_failed guild_id=%s user_id=%s", interaction.guild.id, member.id, exc_info=True)
    else:
        minutes = max(1, int(cfg.get("timeout_minutes", 10)))
        try:
            until = discord.utils.utcnow() + timedelta(minutes=minutes)
            await member.timeout(until, reason=f"Warn threshold reached ({count})")
            await interaction.followup.send(f"Auto action: timeout {member.mention} for {minutes} minutes.", ephemeral=True)
            await add_audit(
                interaction.guild.id,
                interaction.user,
                "moderation_auto_timeout",
                target_id=member.id,
                metadata={"warn_count": int(count), "minutes": int(minutes)},
            )
        except Exception:
            LOGGER.warning("moderation_auto_timeout_failed guild_id=%s user_id=%s", interaction.guild.id, member.id, exc_info=True)


@tree.command(name="warnings", description="Show active warnings for a member")
@app_commands.describe(member="Member to inspect")
async def warnings(interaction: discord.Interaction, member: discord.Member):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("Guild-only command.", ephemeral=True)
        return
    cfg = moderation_cfg(interaction.guild.id)
    if not moderation_command_enabled(interaction.guild.id, cfg, "command_warnings"):
        await interaction.response.send_message("This command is disabled on this server.", ephemeral=True)
        return
    if not has_moderation_access(interaction, cfg):
        await interaction.response.send_message("Missing moderation access.", ephemeral=True)
        return
    if not await enforce_command_rate_limit(interaction, "warnings"):
        return
    count = await count_active_warnings(interaction.guild.id, member.id)
    await interaction.response.send_message(f"{member.mention} has {count} active warnings.", ephemeral=True)


@tree.command(name="clearwarnings", description="Clear active warnings for a member")
@app_commands.describe(member="Member to clear warnings for")
async def clearwarnings(interaction: discord.Interaction, member: discord.Member):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("Guild-only command.", ephemeral=True)
        return
    cfg = moderation_cfg(interaction.guild.id)
    if not moderation_command_enabled(interaction.guild.id, cfg, "command_clearwarnings"):
        await interaction.response.send_message("This command is disabled on this server.", ephemeral=True)
        return
    if not has_moderation_access(interaction, cfg):
        await interaction.response.send_message("Missing moderation access.", ephemeral=True)
        return
    if not await enforce_command_rate_limit(interaction, "clearwarnings"):
        return
    updated = await clear_active_warnings(interaction.guild.id, member.id)
    await interaction.response.send_message(f"Cleared {updated} warnings for {member.mention}.", ephemeral=True)
    await add_audit(
        interaction.guild.id,
        interaction.user,
        "moderation_clearwarnings",
        target_id=member.id,
        metadata={"cleared_count": int(updated)},
    )


@tree.command(name="timeout", description="Timeout a member for a number of minutes")
@app_commands.describe(member="Member to timeout", minutes="Timeout length in minutes", reason="Reason")
async def timeout(interaction: discord.Interaction, member: discord.Member, minutes: int = 10, reason: str = "No reason provided"):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("Guild-only command.", ephemeral=True)
        return
    cfg = moderation_cfg(interaction.guild.id)
    if not moderation_command_enabled(interaction.guild.id, cfg, "command_timeout"):
        await interaction.response.send_message("This command is disabled on this server.", ephemeral=True)
        return
    if not await enforce_command_rate_limit(interaction, "timeout"):
        return
    ok, err = PERMISSION_SERVICE.validate_moderation_action(
        interaction.user,
        member,
        "timeout",
        {str(x) for x in cfg.get("moderator_role_ids", []) if str(x).isdigit()},
    )
    if not ok:
        await interaction.response.send_message(err, ephemeral=True)
        return
    minutes = max(1, min(40320, int(minutes)))
    until = discord.utils.utcnow() + timedelta(minutes=minutes)
    await member.timeout(until, reason=reason[:500])
    await interaction.response.send_message(f"Timed out {member.mention} for {minutes} minutes.", ephemeral=False)
    await add_audit(
        interaction.guild.id,
        interaction.user,
        "moderation_timeout",
        target_id=member.id,
        metadata={"minutes": int(minutes), "reason": str(reason)[:300]},
    )
    await METRICS.track(str(interaction.guild.id), "timeout", {"user_id": member.id, "moderator_id": interaction.user.id})


@tree.command(name="kick", description="Kick a member")
@app_commands.describe(member="Member to kick", reason="Reason")
async def kick(interaction: discord.Interaction, member: discord.Member, reason: str = "No reason provided"):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("Guild-only command.", ephemeral=True)
        return
    cfg = moderation_cfg(interaction.guild.id)
    if not moderation_command_enabled(interaction.guild.id, cfg, "command_kick"):
        await interaction.response.send_message("This command is disabled on this server.", ephemeral=True)
        return
    if not await enforce_command_rate_limit(interaction, "kick"):
        return
    ok, err = PERMISSION_SERVICE.validate_moderation_action(
        interaction.user,
        member,
        "kick",
        {str(x) for x in cfg.get("moderator_role_ids", []) if str(x).isdigit()},
    )
    if not ok:
        await interaction.response.send_message(err, ephemeral=True)
        return
    await member.kick(reason=reason[:500])
    await interaction.response.send_message(f"Kicked {member.mention}.", ephemeral=False)
    await add_audit(
        interaction.guild.id,
        interaction.user,
        "moderation_kick",
        target_id=member.id,
        metadata={"reason": str(reason)[:300]},
    )
    await METRICS.track(str(interaction.guild.id), "kick", {"user_id": member.id, "moderator_id": interaction.user.id})


@tree.command(name="ban", description="Ban a member")
@app_commands.describe(member="Member to ban", reason="Reason", delete_days="Delete message history days (0-7)")
async def ban(interaction: discord.Interaction, member: discord.Member, reason: str = "No reason provided", delete_days: int = 0):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("Guild-only command.", ephemeral=True)
        return
    cfg = moderation_cfg(interaction.guild.id)
    if not moderation_command_enabled(interaction.guild.id, cfg, "command_ban"):
        await interaction.response.send_message("This command is disabled on this server.", ephemeral=True)
        return
    if not await enforce_command_rate_limit(interaction, "ban"):
        return
    ok, err = PERMISSION_SERVICE.validate_moderation_action(
        interaction.user,
        member,
        "ban",
        {str(x) for x in cfg.get("moderator_role_ids", []) if str(x).isdigit()},
    )
    if not ok:
        await interaction.response.send_message(err, ephemeral=True)
        return
    delete_days = max(0, min(7, int(delete_days)))
    await member.ban(reason=reason[:500], delete_message_days=delete_days)
    await interaction.response.send_message(f"Banned {member.mention}.", ephemeral=False)
    await add_audit(
        interaction.guild.id,
        interaction.user,
        "moderation_ban",
        target_id=member.id,
        metadata={"reason": str(reason)[:300], "delete_days": int(delete_days)},
    )
    await METRICS.track(str(interaction.guild.id), "ban", {"user_id": member.id, "moderator_id": interaction.user.id})


@tree.command(name="reloadconfig", description="Reload guild configuration")
async def reloadconfig(interaction: discord.Interaction):
    try:
        if not interaction.guild:
            await interaction.response.send_message("Guild-only command.", ephemeral=True)
            return
        if not command_enabled_for_guild(interaction.guild.id, "command_reloadconfig", default=True):
            await interaction.response.send_message("This command is disabled on this server.", ephemeral=True)
            return
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Missing MANAGE_GUILD permission.", ephemeral=True)
            return
        if not await enforce_command_rate_limit(interaction, "reloadconfig"):
            return

        _ = CONFIG_ENGINE.get_all()
        REGISTRY.hot_reload()
        await _run_with_timeout(
            CUSTOM_COMMAND_SERVICE.sync(tree, CONFIG_ENGINE),
            "custom_commands_sync_reload",
            timeout_sec=TREE_SYNC_TIMEOUT_SEC,
        )
        await interaction.response.send_message("Config and modules reloaded.", ephemeral=True)
    except Exception:
        LOGGER.exception("reload_config_failed guild_id=%s", interaction.guild.id if interaction.guild else 0)
        if not interaction.response.is_done():
            await interaction.response.send_message("Reload failed.", ephemeral=True)


@tree.command(name="modules_health", description="Show module health")
async def modules_health(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Guild-only command.", ephemeral=True)
        return
    if not command_enabled_for_guild(interaction.guild.id, "command_modules_health", default=True):
        await interaction.response.send_message("This command is disabled on this server.", ephemeral=True)
        return
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message("Missing MANAGE_GUILD permission.", ephemeral=True)
        return
    if not await enforce_command_rate_limit(interaction, "modules_health"):
        return

    health = REGISTRY.health()
    lines = [f"{k}: {'ok' if v['healthy'] else 'error'} ({v['version']})" for k, v in health.items()]
    await interaction.response.send_message("\n".join(lines[:20]) or "No modules", ephemeral=True)


ticket_group = app_commands.Group(name="ticket", description="Support ticket commands")


@ticket_group.command(name="open", description="Open a private support ticket")
@app_commands.describe(reason="What you need help with")
async def ticket_open(interaction: discord.Interaction, reason: str = "No reason provided"):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("Guild-only command.", ephemeral=True)
        return
    if not command_enabled_for_guild(interaction.guild.id, "command_ticket_open", default=True):
        await interaction.response.send_message("This command is disabled on this server.", ephemeral=True)
        return
    if not await enforce_command_rate_limit(interaction, "ticket_open"):
        return

    channel, status = await _open_ticket_for_member(interaction.guild, interaction.user, reason)
    if status == "missing_permissions":
        await interaction.response.send_message("Bot is missing Manage Channels permission.", ephemeral=True)
        return
    if channel is None:
        await interaction.response.send_message("Could not open ticket.", ephemeral=True)
        return
    if status == "existing":
        await interaction.response.send_message(f"You already have an open ticket: {channel.mention}", ephemeral=True)
        return
    await interaction.response.send_message(f"Ticket opened: {channel.mention}", ephemeral=True)
    await METRICS.track(str(interaction.guild.id), "ticket_open", {"user_id": interaction.user.id, "channel_id": channel.id})
    await add_audit(
        interaction.guild.id,
        interaction.user,
        "ticket_open",
        target_id=channel.id,
        metadata={"source": "slash", "reason": str(reason)[:300]},
    )
    REALTIME_BUS.publish(str(interaction.guild.id), {"type": "ticket_open", "user_id": interaction.user.id, "channel_id": channel.id})


@ticket_group.command(name="close", description="Close the current ticket channel")
@app_commands.describe(reason="Closing reason")
async def ticket_close(interaction: discord.Interaction, reason: str = "Resolved"):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("Guild-only command.", ephemeral=True)
        return
    if not command_enabled_for_guild(interaction.guild.id, "command_ticket_close", default=True):
        await interaction.response.send_message("This command is disabled on this server.", ephemeral=True)
        return
    if not await enforce_command_rate_limit(interaction, "ticket_close"):
        return
    if not isinstance(interaction.channel, discord.TextChannel) or not interaction.channel.name.startswith("ticket-"):
        await interaction.response.send_message("Run this in a ticket channel.", ephemeral=True)
        return

    if not _can_close_ticket(interaction.user, interaction.guild):
        await interaction.response.send_message("Only admins or ticket staff can close this ticket.", ephemeral=True)
        return
    owner_id = _ticket_owner_id_from_topic(interaction.channel.topic or "")

    ch = interaction.channel
    await interaction.response.send_message("Closing ticket in 3 seconds...", ephemeral=False)
    await METRICS.track(
        str(interaction.guild.id),
        "ticket_close",
        {"user_id": interaction.user.id, "channel_id": ch.id, "ticket_owner_id": owner_id or 0},
    )
    await add_audit(
        interaction.guild.id,
        interaction.user,
        "ticket_close",
        target_id=ch.id,
        metadata={"source": "slash", "reason": str(reason)[:300], "ticket_owner_id": int(owner_id or 0)},
    )
    REALTIME_BUS.publish(str(interaction.guild.id), {"type": "ticket_close", "user_id": interaction.user.id, "channel_id": ch.id})
    await asyncio.sleep(3)
    await _archive_ticket_and_delete(ch, interaction.user, reason)


@ticket_group.command(name="add", description="Add a member to this ticket")
@app_commands.describe(member="Member to add")
async def ticket_add(interaction: discord.Interaction, member: discord.Member):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("Guild-only command.", ephemeral=True)
        return
    if not command_enabled_for_guild(interaction.guild.id, "command_ticket_add", default=True):
        await interaction.response.send_message("This command is disabled on this server.", ephemeral=True)
        return
    if not isinstance(interaction.channel, discord.TextChannel) or not interaction.channel.name.startswith("ticket-"):
        await interaction.response.send_message("Run this in a ticket channel.", ephemeral=True)
        return
    if not await enforce_command_rate_limit(interaction, "ticket_add"):
        return
    cfg = moderation_cfg(interaction.guild.id)
    if not (has_moderation_access(interaction, cfg) or interaction.user.guild_permissions.manage_channels):
        await interaction.response.send_message("Missing moderation access.", ephemeral=True)
        return
    await interaction.channel.set_permissions(
        member,
        view_channel=True,
        send_messages=True,
        read_message_history=True,
        attach_files=True,
        embed_links=True,
        reason=f"ticket add by {interaction.user}",
    )
    await interaction.response.send_message(f"Added {member.mention} to this ticket.", ephemeral=False)
    await add_audit(
        interaction.guild.id,
        interaction.user,
        "ticket_member_add",
        target_id=member.id,
        metadata={"channel_id": int(interaction.channel.id)},
    )
    await _send_ticket_log(
        interaction.guild,
        title="Ticket Member Added",
        description=f"Ticket: {interaction.channel.mention}\nAdded: {member.mention}\nBy: {interaction.user.mention}",
        color=discord.Colour.gold(),
    )


@ticket_group.command(name="remove", description="Remove a member from this ticket")
@app_commands.describe(member="Member to remove")
async def ticket_remove(interaction: discord.Interaction, member: discord.Member):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("Guild-only command.", ephemeral=True)
        return
    if not command_enabled_for_guild(interaction.guild.id, "command_ticket_remove", default=True):
        await interaction.response.send_message("This command is disabled on this server.", ephemeral=True)
        return
    if not isinstance(interaction.channel, discord.TextChannel) or not interaction.channel.name.startswith("ticket-"):
        await interaction.response.send_message("Run this in a ticket channel.", ephemeral=True)
        return
    if not await enforce_command_rate_limit(interaction, "ticket_remove"):
        return
    cfg = moderation_cfg(interaction.guild.id)
    if not (has_moderation_access(interaction, cfg) or interaction.user.guild_permissions.manage_channels):
        await interaction.response.send_message("Missing moderation access.", ephemeral=True)
        return
    await interaction.channel.set_permissions(member, overwrite=None, reason=f"ticket remove by {interaction.user}")
    await interaction.response.send_message(f"Removed {member.mention} from this ticket.", ephemeral=False)
    await add_audit(
        interaction.guild.id,
        interaction.user,
        "ticket_member_remove",
        target_id=member.id,
        metadata={"channel_id": int(interaction.channel.id)},
    )
    await _send_ticket_log(
        interaction.guild,
        title="Ticket Member Removed",
        description=f"Ticket: {interaction.channel.mention}\nRemoved: {member.mention}\nBy: {interaction.user.mention}",
        color=discord.Colour.orange(),
    )


tree.add_command(ticket_group)


def main():
    global DASHBOARD_THREAD
    CONFIG_ENGINE.on_reload = _on_config_reload
    _validate_startup_dependencies()
    set_module_health_provider(REGISTRY.health)
    set_scheduler_provider(SCHEDULER)
    DASHBOARD_THREAD = threading.Thread(target=run_dashboard, daemon=True)
    DASHBOARD_THREAD.start()
    start_ws_server("127.0.0.1", 8765, REALTIME_BUS, WS_AUTH)

    def _handle_shutdown(signum, _frame):
        LOGGER.warning("shutdown_signal_received signum=%s", signum)
        try:
            SCHEDULER.stop()
        except Exception:
            LOGGER.warning("scheduler_stop_failed", exc_info=True)
        try:
            if bot.is_ready():
                asyncio.run_coroutine_threadsafe(bot.close(), bot.loop)
        except Exception:
            LOGGER.warning("bot_shutdown_failed", exc_info=True)

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)
    bot.run(TOKEN, log_handler=None)


if __name__ == "__main__":
    main()
