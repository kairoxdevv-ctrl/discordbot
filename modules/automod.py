"""Feature module: automatic moderation checks and violation handling."""

VERSION = "1.1.0"
import logging
import re
from collections import deque
from time import monotonic
from urllib.parse import urlparse

from . import logging as log_module

DEFAULT = {
    "enabled": False,
    "anti_invite": True,
    "anti_links": False,
    "allowed_domains": [],
    "anti_spam": True,
    "anti_mass_mentions": False,
    "mention_threshold": 5,
    "anti_caps": False,
    "caps_threshold": 80,
    "max_message_length": 0,
    "block_attachments": False,
    "max_attachment_size_mb": 8,
    "exempt_role_ids": [],
    "exempt_channel_ids": [],
    "spam_threshold": 5,
    "spam_window_seconds": 8,
    "banned_words": [],
    "word_match_mode": "contains",
    "case_sensitive": False,
    "delete_message": True,
    "send_warning": True,
    "warning_delete_after_seconds": 6,
    "log_violations": True,
}

INVITE_RE = re.compile(r"(?:discord\\.gg|discord\\.com/invite)/[A-Za-z0-9-]+", re.IGNORECASE)
URL_RE = re.compile(r"(https?://[^\s]+|www\.[^\s]+)", re.IGNORECASE)


class SpamTracker:
    """Sliding-window spam counter for per-user message burst detection."""

    def __init__(self):
        self._cache = {}
        self._last_cleanup = 0.0

    def add_and_check(self, guild_id: int, user_id: int, window: int, threshold: int) -> bool:
        """Record message hit and return True when spam threshold is reached."""
        key = (guild_id, user_id)
        now = monotonic()
        q = self._cache.setdefault(key, deque())
        q.append(now)
        cutoff = now - max(2, window)
        while q and q[0] < cutoff:
            q.popleft()
        if not q:
            self._cache.pop(key, None)
            return False
        if (now - self._last_cleanup) > 300:
            stale_before = now - max(2, window)
            stale_keys = []
            for k, dq in self._cache.items():
                while dq and dq[0] < stale_before:
                    dq.popleft()
                if not dq:
                    stale_keys.append(k)
            for k in stale_keys:
                self._cache.pop(k, None)
            self._last_cleanup = now
        return len(q) >= max(2, threshold)


spam_tracker = SpamTracker()
LOGGER = logging.getLogger("discordbot.modules.automod")


def _is_exempt(message, cfg: dict) -> bool:
    channel_ids = {str(x) for x in cfg.get("exempt_channel_ids", []) if str(x).isdigit()}
    if channel_ids and str(message.channel.id) in channel_ids:
        return True
    role_ids = {str(x) for x in cfg.get("exempt_role_ids", []) if str(x).isdigit()}
    if role_ids:
        for role in getattr(message.author, "roles", []) or []:
            if str(getattr(role, "id", "")) in role_ids:
                return True
    return False


def _link_violation(content: str, cfg: dict) -> str:
    matches = URL_RE.findall(content or "")
    if not matches:
        return ""
    if not cfg.get("anti_links", False):
        return ""
    allowed = {str(x).strip().lower() for x in cfg.get("allowed_domains", []) if str(x).strip()}
    if not allowed:
        return "Links are not allowed"
    for raw in matches:
        candidate = raw if "://" in raw else f"http://{raw}"
        host = (urlparse(candidate).hostname or "").lower()
        if not host:
            continue
        if any(host == d or host.endswith(f".{d}") for d in allowed):
            continue
        return "Link domain is not allowed"
    return ""


def _word_hit(content: str, banned_words: list[str], mode: str, case_sensitive: bool) -> str:
    if not case_sensitive:
        content = content.lower()
    for word in banned_words:
        w = str(word).strip()
        if not w:
            continue
        needle = w if case_sensitive else w.lower()
        if mode == "whole_word":
            if re.search(rf"\b{re.escape(needle)}\b", content):
                return w
        elif needle in content:
            return w
    return ""


async def on_message(message, cfg: dict, logging_cfg: dict):
    if not cfg.get("enabled", False):
        return
    if not message.guild or message.author.bot:
        return
    if _is_exempt(message, cfg):
        return False

    content = message.content or ""
    violation = ""

    if cfg.get("anti_invite", True) and INVITE_RE.search(content):
        violation = "Invite links are not allowed"

    if not violation:
        violation = _link_violation(content, cfg)

    if not violation and cfg.get("anti_spam", True):
        if spam_tracker.add_and_check(
            guild_id=message.guild.id,
            user_id=message.author.id,
            window=int(cfg.get("spam_window_seconds", 8)),
            threshold=int(cfg.get("spam_threshold", 5)),
        ):
            violation = "Spam detected"

    if not violation and cfg.get("anti_mass_mentions", False):
        if len(getattr(message, "mentions", []) or []) >= int(cfg.get("mention_threshold", 5)):
            violation = "Too many mentions in one message"

    if not violation and cfg.get("anti_caps", False):
        letters = [c for c in content if c.isalpha()]
        if len(letters) >= 12:
            upper_ratio = int((sum(1 for c in letters if c.isupper()) * 100) / len(letters))
            if upper_ratio >= int(cfg.get("caps_threshold", 80)):
                violation = "Too many capital letters"

    if not violation:
        max_len = int(cfg.get("max_message_length", 0) or 0)
        if max_len > 0 and len(content) > max_len:
            violation = f"Message too long ({len(content)}>{max_len})"

    if not violation and getattr(message, "attachments", None):
        if cfg.get("block_attachments", False):
            violation = "Attachments are not allowed"
        else:
            max_mb = int(cfg.get("max_attachment_size_mb", 8) or 0)
            if max_mb > 0:
                max_bytes = max_mb * 1024 * 1024
                for attachment in message.attachments:
                    if int(getattr(attachment, "size", 0) or 0) > max_bytes:
                        violation = f"Attachment too large (>{max_mb}MB)"
                        break

    if not violation:
        hit = _word_hit(
            content,
            cfg.get("banned_words", []),
            str(cfg.get("word_match_mode", "contains")),
            bool(cfg.get("case_sensitive", False)),
        )
        if hit:
            violation = f"Blocked word detected: {hit}"

    if not violation:
        return False

    if cfg.get("delete_message", True):
        try:
            await message.delete()
        except Exception:
            LOGGER.warning("automod_delete_failed guild_id=%s message_id=%s", message.guild.id, message.id, exc_info=True)

    if cfg.get("send_warning", True):
        try:
            await message.channel.send(
                f"{message.author.mention} {violation}.",
                delete_after=int(cfg.get("warning_delete_after_seconds", 6)),
            )
        except Exception:
            LOGGER.warning(
                "automod_warn_send_failed guild_id=%s channel_id=%s user_id=%s",
                message.guild.id,
                message.channel.id,
                message.author.id,
                exc_info=True,
            )

    if cfg.get("log_violations", True):
        await log_module.send_log(
            message.guild,
            logging_cfg,
            "Automod Violation",
            f"User: {message.author.mention}\nChannel: {message.channel.mention}\nReason: {violation}",
        )
    return True
