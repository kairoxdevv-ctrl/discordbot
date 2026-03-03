"""Feature module: dynamic custom slash command registration and execution."""

VERSION = "1.1.0"
import logging
import os
import re
import time

import discord
from discord import app_commands
from discord.enums import AppCommandType

DEFAULT = {
    "enabled": False,
    "commands": []
}

NAME_RE = re.compile(r"^[a-z0-9_]{1,32}$")
RESERVED_NAMES = {
    "reloadconfig",
    "modules_health",
    "warn",
    "warnings",
    "clearwarnings",
    "timeout",
    "kick",
    "ban",
}

LOGGER = logging.getLogger("discordbot.custom_commands")
MAX_CUSTOM_COMMANDS = max(1, min(200, int(os.getenv("MAX_CUSTOM_COMMANDS_PER_GUILD", "50"))))
MAX_CUSTOM_RESPONSE_LEN = max(10, min(4000, int(os.getenv("MAX_CUSTOM_RESPONSE_LEN", "1500"))))


class CustomCommandManager:
    """Feature-layer manager for guild-scoped custom slash commands.

    Responsibilities:
    - Sanitize command definitions from config.
    - Register/unregister commands into command tree.
    - Enforce lightweight per-user cooldown at callback runtime.
    Not responsible for dashboard permission/auth decisions.
    """

    def __init__(self):
        self._registered = {}
        self._cooldowns = {}
        self._cooldown_sec = max(1, int(os.getenv("CUSTOM_COMMAND_COOLDOWN_SEC", "3")))

    @staticmethod
    def sanitize(commands: list[dict]) -> list[dict]:
        """Normalize raw custom command rows into safe command definitions."""
        sanitized = []
        for item in commands:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip().lower()
            description = str(item.get("description", "")).strip()
            response = str(item.get("response", "")).strip()
            if not NAME_RE.fullmatch(name):
                continue
            if name in RESERVED_NAMES:
                continue
            if not description:
                description = f"Run {name}"
            description = description[:100]
            if not response:
                continue
            response = response[:MAX_CUSTOM_RESPONSE_LEN]
            sanitized.append({"name": name, "description": description, "response": response})
        return sanitized[:MAX_CUSTOM_COMMANDS]

    async def sync(self, tree: app_commands.CommandTree, config_store):
        """Reconcile command tree registrations with persisted config state."""
        for guild_id, command_names in list(self._registered.items()):
            for command_name in command_names:
                cmd = tree.get_command(command_name, guild=discord.Object(id=int(guild_id)))
                if cmd:
                    tree.remove_command(command_name, guild=discord.Object(id=int(guild_id)), type=AppCommandType.chat_input)
        self._registered.clear()

        data = config_store.get_all()
        for gid, payload in data.items():
            if not str(gid).isdigit():
                continue
            modules = payload.get("modules", {})
            cc = modules.get("custom_commands", {})
            command_cfg = modules.get("commands", {})

            if isinstance(command_cfg, dict):
                if not command_cfg.get("enabled", True):
                    continue
                if not command_cfg.get("custom_commands_enabled", True):
                    continue

            if not cc.get("enabled", False):
                continue
            commands = self.sanitize(cc.get("commands", []))
            if not commands:
                continue

            max_commands = 20
            if isinstance(command_cfg, dict):
                try:
                    max_commands = int(command_cfg.get("custom_commands_max", 20))
                except Exception:
                    max_commands = 20
            max_commands = max(1, min(MAX_CUSTOM_COMMANDS, max_commands))
            commands = commands[:max_commands]
            ephemeral = bool(command_cfg.get("custom_response_ephemeral", False)) if isinstance(command_cfg, dict) else False

            guild_obj = discord.Object(id=int(gid))
            added = []
            for item in commands:
                name = item["name"]
                description = item["description"]
                response = item["response"]

                def _make_callback(text: str, is_ephemeral: bool, command_name: str):
                    async def _callback(interaction: discord.Interaction):
                        key = (
                            int(getattr(interaction.guild, "id", 0) or 0),
                            int(getattr(interaction.user, "id", 0) or 0),
                            str(command_name),
                        )
                        now = time.time()
                        until = float(self._cooldowns.get(key, 0.0))
                        if now < until:
                            await interaction.response.send_message(
                                f"Slow down. Try again in {max(1, int(until - now))}s.",
                                ephemeral=True,
                            )
                            return
                        self._cooldowns[key] = now + self._cooldown_sec
                        if len(self._cooldowns) > 20000:
                            cutoff = now - (self._cooldown_sec * 2)
                            stale = [k for k, v in self._cooldowns.items() if v < cutoff]
                            for stale_key in stale:
                                self._cooldowns.pop(stale_key, None)
                        await interaction.response.send_message(text, ephemeral=is_ephemeral)
                    return _callback

                try:
                    cmd = app_commands.Command(
                        name=name,
                        description=description,
                        callback=_make_callback(response, ephemeral, name),
                    )
                    tree.add_command(cmd, guild=guild_obj, override=True)
                    added.append(name)
                except Exception:
                    LOGGER.warning("custom_command_register_failed guild_id=%s command=%s", gid, name, exc_info=True)

            if added:
                self._registered[gid] = added
                try:
                    await tree.sync(guild=guild_obj)
                except Exception:
                    LOGGER.warning("custom_command_sync_failed guild_id=%s", gid, exc_info=True)
