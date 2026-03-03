"""Presentation layer for HTTP dashboard routes, auth flow, and API endpoints."""

import asyncio
import json
import logging
import os
import re
import threading
import time
from collections import deque
from pathlib import Path
from queue import Empty
from urllib.parse import urlencode

import psutil
import requests
import discord
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, redirect, render_template, request, send_from_directory, session, url_for
from waitress import serve
from werkzeug.middleware.proxy_fix import ProxyFix

from core.config_engine import ConfigEngineV3
from core.logging_setup import setup_structured_logging
from core.metrics import MetricsEngine
from core.realtime import RealtimeBus
from core.security import RateLimiter, issue_csrf, sanitize_id, sanitize_text, validate_csrf
from core.storage import Repository, SQLitePool
from core.ws_server import WsAuthManager
from modules import automod, autorole, logging as log_module, welcome
from repositories.config_repository import ConfigRepository
from repositories.support_repository import SupportRepository
from services.config_service import ConfigService
from services.permission_service import PermissionService
from services.realtime_service import RealtimeService
from services.support_service import SupportService

BASE_DIR = Path("/root/discordbot")
CONFIG_PATH = BASE_DIR / "config.json"
DB_PATH = BASE_DIR / "platform.db"
ENV_PATH = BASE_DIR / ".env"

load_dotenv(ENV_PATH)
setup_structured_logging(os.getenv("LOG_LEVEL", "INFO"))

CLIENT_ID = os.getenv("CLIENT_ID", "").strip()
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "").strip()
REDIRECT_URI = os.getenv("REDIRECT_URI", "").strip()
SECRET_KEY = os.getenv("SECRET_KEY", "").strip()
OWNER_ID = os.getenv("OWNER_ID", "").strip()
SUPPORT_ADMIN_IDS = {
    x.strip()
    for x in str(os.getenv("SUPPORT_ADMIN_IDS", "") or "").split(",")
    if x.strip().isdigit()
}
SUPPORT_ALERT_WEBHOOK_URL = os.getenv("SUPPORT_ALERT_WEBHOOK_URL", "").strip()
VERIFY_BASE_URL = os.getenv("VERIFY_BASE_URL", "https://stupid.linkpc.net").strip().rstrip("/")
MAX_JSON_BODY_BYTES = max(1024, min(2 * 1024 * 1024, int(os.getenv("MAX_JSON_BODY_BYTES", "262144"))))
ALLOWED_ORIGINS = {
    x.strip().lower().rstrip("/")
    for x in str(os.getenv("ALLOWED_ORIGINS", "") or "").split(",")
    if x.strip()
}
SAFE_TEXT_RE = re.compile(r"^[\w\s\-\.,:;!?@#&()/+*'\"<>=%]*$")
MAX_CUSTOM_COMMANDS_PER_GUILD = max(1, min(200, int(os.getenv("MAX_CUSTOM_COMMANDS_PER_GUILD", "50"))))
MAX_CUSTOM_RESPONSE_LEN = max(10, min(4000, int(os.getenv("MAX_CUSTOM_RESPONSE_LEN", "1500"))))

MANAGE_GUILD = 1 << 5
GUILDS_REFRESH_TTL_SEC = max(5, int(os.getenv("GUILDS_REFRESH_TTL_SEC", "45") or "45"))
GUILD_FEATURE_FLAGS = {
    "welcome": True,
    "autorole": True,
    "automod": True,
    "logging": True,
    "custom_commands": True,
    "commands": True,
    "moderation_tools": True,
    "reaction_roles": True,
    "leveling_system": True,
    "webhooks": True,
    "integrations": True,
    "premium": True,
    "settings": True,
}


def defaults_factory():
    return {
        "schema_version": 3,
        "modules": {
            "welcome": dict(welcome.DEFAULT),
            "autorole": dict(autorole.DEFAULT),
            "automod": dict(automod.DEFAULT),
            "logging": dict(log_module.DEFAULT),
            "custom_commands": {"enabled": False, "commands": []},
            "commands": {
                "enabled": True,
                "custom_commands_enabled": True,
                "custom_commands_max": 20,
                "custom_response_ephemeral": False,
                "command_reloadconfig": True,
                "command_modules_health": True,
                "command_warn": True,
                "command_warnings": True,
                "command_clearwarnings": True,
                "command_timeout": True,
                "command_kick": False,
                "command_ban": False,
            },
            "moderation_tools": {
                "enabled": False,
                "warn_threshold": 3,
                "timeout_minutes": 10,
                "auto_kick": False,
                "moderator_role_ids": [],
                "command_warn": True,
                "command_warnings": True,
                "command_clearwarnings": True,
                "command_timeout": True,
                "command_kick": False,
                "command_ban": False,
            },
            "reaction_roles": {
                "enabled": False,
                "channel_id": "",
                "message_id": "",
                "message_template": "React below to get your roles.",
                "show_role_list": True,
                "pairs": [],
            },
            "leveling_system": {
                "enabled": False,
                "xp_per_message": 10,
                "cooldown_seconds": 30,
                "level_up_channel_id": "",
            },
            "webhooks": {"enabled": False, "notify_url": "", "events": []},
            "integrations": {"enabled": False, "api_base_url": "", "api_key_hint": "", "sync_interval_minutes": 15},
            "premium": {"enabled": False, "tier": "free", "limits_override": False},
            "settings": {
                "enabled": True,
                "language": "en",
                "timezone": "UTC",
                "alerts_enabled": True,
                "verify_enabled": True,
                "lock_enabled": False,
                "verified_role_id": "",
                "verify_channel_id": "",
                "lock_mode": "all_except_verify",
                "verify_lock_category_ids": [],
                "verify_lock_last_applied_channel_ids": [],
                "verify_lock_last_applied_role_ids": [],
                "strict_hide_unverified": False,
                "ticket_panel_channel_id": "",
                "ticket_panel_title": "Need Support?",
                "ticket_panel_body": "New here or need help? Click the button below to open a private support ticket.",
                "ticket_panel_message_id": "",
                "verify_role_id": "",
                "verify_min_account_age_days": 7,
                "verify_panel_channel_id": "",
                "verify_panel_title": "Server Verification",
                "verify_panel_body": "Click the button below to verify and unlock full access.",
                "verify_panel_message_id": "",
                "verify_visible_channel_ids": [],
            },
        },
    }


CONFIG_ENGINE = ConfigEngineV3(CONFIG_PATH, defaults_factory)
POOL = SQLitePool(DB_PATH, size=4)
POOL.init_schema()
REPO = Repository(POOL)
METRICS = MetricsEngine(REPO)
RATE_LIMITER = RateLimiter()
REALTIME_BUS = RealtimeBus()
WS_AUTH = WsAuthManager()
LOGGER = logging.getLogger("discordbot.dashboard")
MODULE_HEALTH_PROVIDER = None
SCHEDULER_PROVIDER = None
PROCESS = psutil.Process(os.getpid())
STARTED_AT = int(time.time())
MEMORY_SAMPLES = deque(maxlen=24)
CONFIG_SERVICE = ConfigService(ConfigRepository(CONFIG_ENGINE), ttl_sec=max(5, int(os.getenv("CONFIG_CACHE_TTL_SEC", "30"))))
PERMISSION_SERVICE = PermissionService()
REALTIME_SERVICE = RealtimeService(REALTIME_BUS, WS_AUTH)
SUPPORT_REPOSITORY = None
SUPPORT_SERVICE = None


def _ensure_support_schema():
    conn = POOL.acquire()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS support_cases (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              guild_id TEXT NOT NULL,
              guild_name TEXT NOT NULL DEFAULT '',
              created_by_id TEXT NOT NULL,
              created_by_name TEXT NOT NULL,
              subject TEXT NOT NULL,
              priority TEXT NOT NULL DEFAULT 'normal',
              status TEXT NOT NULL DEFAULT 'open',
              assigned_to_id TEXT NOT NULL DEFAULT '',
              assigned_to_name TEXT NOT NULL DEFAULT '',
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL,
              last_message_at INTEGER NOT NULL,
              last_message_preview TEXT NOT NULL DEFAULT '',
              last_actor_role TEXT NOT NULL DEFAULT 'requester',
              requester_last_message_at INTEGER NOT NULL DEFAULT 0,
              support_last_message_at INTEGER NOT NULL DEFAULT 0,
              first_response_at INTEGER NOT NULL DEFAULT 0,
              resolved_at INTEGER NOT NULL DEFAULT 0,
              sla_alert_sent_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_support_cases_guild_status ON support_cases (guild_id, status, updated_at DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_support_cases_status_updated ON support_cases (status, updated_at DESC)"
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS support_messages (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              case_id INTEGER NOT NULL,
              guild_id TEXT NOT NULL,
              author_id TEXT NOT NULL,
              author_name TEXT NOT NULL,
              author_role TEXT NOT NULL,
              body TEXT NOT NULL,
              visibility TEXT NOT NULL DEFAULT 'public',
              created_at INTEGER NOT NULL,
              FOREIGN KEY(case_id) REFERENCES support_cases(id) ON DELETE CASCADE
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_support_messages_case_time ON support_messages (case_id, created_at ASC)"
        )
        cur.execute("PRAGMA table_info(support_cases)")
        case_cols = {str(r[1]) for r in cur.fetchall()}
        for col, definition in (
            ("last_actor_role", "TEXT NOT NULL DEFAULT 'requester'"),
            ("requester_last_message_at", "INTEGER NOT NULL DEFAULT 0"),
            ("support_last_message_at", "INTEGER NOT NULL DEFAULT 0"),
            ("first_response_at", "INTEGER NOT NULL DEFAULT 0"),
            ("resolved_at", "INTEGER NOT NULL DEFAULT 0"),
            ("sla_alert_sent_at", "INTEGER NOT NULL DEFAULT 0"),
        ):
            if col not in case_cols:
                cur.execute(f"ALTER TABLE support_cases ADD COLUMN {col} {definition}")
        cur.execute("PRAGMA table_info(support_messages)")
        msg_cols = {str(r[1]) for r in cur.fetchall()}
        if "visibility" not in msg_cols:
            cur.execute("ALTER TABLE support_messages ADD COLUMN visibility TEXT NOT NULL DEFAULT 'public'")
        conn.commit()
    finally:
        POOL.release(conn)


def _db_fetchall_sync(sql: str, params=()):
    conn = POOL.acquire()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]
    finally:
        POOL.release(conn)


def _db_fetchone_sync(sql: str, params=()):
    rows = _db_fetchall_sync(sql, params)
    return rows[0] if rows else None


def _db_execute_sync(sql: str, params=()):
    conn = POOL.acquire()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        return cur.lastrowid, cur.rowcount
    finally:
        POOL.release(conn)


def _support_sla_deadline_sec(priority: str) -> int:
    p = str(priority or "normal").lower()
    if p == "urgent":
        return 15 * 60
    if p == "high":
        return 60 * 60
    if p == "low":
        return 12 * 60 * 60
    return 4 * 60 * 60


def _support_is_sla_breached(case_row: dict, now: int | None = None) -> bool:
    ts_now = int(time.time()) if now is None else int(now)
    if str(case_row.get("status", "")) not in {"open", "in_progress"}:
        return False
    if str(case_row.get("last_actor_role", "requester")) != "requester":
        return False
    last_message_at = int(case_row.get("last_message_at", 0) or 0)
    wait_sec = max(0, ts_now - last_message_at)
    return wait_sec > _support_sla_deadline_sec(str(case_row.get("priority", "normal")))


def _support_send_webhook(event_name: str, case_row: dict, extra_text: str = ""):
    if not SUPPORT_ALERT_WEBHOOK_URL:
        return
    try:
        case_id = int(case_row.get("id", 0) or 0)
        guild_name = str(case_row.get("guild_name", "") or case_row.get("guild_id", "unknown"))
        priority = str(case_row.get("priority", "normal"))
        status = str(case_row.get("status", "open"))
        subject = str(case_row.get("subject", ""))
        created_by = str(case_row.get("created_by_name", "user"))
        color = 15158332 if priority == "urgent" else 16098851
        payload = {
            "username": "Bot Support Alerts",
            "embeds": [
                {
                    "title": f"Support Alert: {event_name}",
                    "description": subject[:280],
                    "color": color,
                    "fields": [
                        {"name": "Case", "value": f"#{case_id}", "inline": True},
                        {"name": "Guild", "value": guild_name[:100], "inline": True},
                        {"name": "Priority", "value": priority, "inline": True},
                        {"name": "Status", "value": status, "inline": True},
                        {"name": "Created By", "value": created_by[:100], "inline": True},
                        {"name": "Details", "value": (extra_text or "n/a")[:350], "inline": False},
                    ],
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
            ],
        }
        requests.post(SUPPORT_ALERT_WEBHOOK_URL, json=payload, timeout=10)
    except Exception:
        LOGGER.warning("support_alert_webhook_failed event=%s", event_name, exc_info=True)


def _support_maybe_send_sla_alert(case_id: int):
    if SUPPORT_REPOSITORY is None or SUPPORT_SERVICE is None:
        return
    row = SUPPORT_REPOSITORY.get_case(int(case_id))
    if not row:
        return
    SUPPORT_SERVICE.maybe_send_sla_alert(row, _support_is_sla_breached)


_ensure_support_schema()


def _memory_monitor_loop():
    while True:
        try:
            rss = int(PROCESS.memory_info().rss)
            MEMORY_SAMPLES.append((int(time.time()), rss))
        except Exception:
            LOGGER.warning("memory_monitor_sample_failed", exc_info=True)
        time.sleep(300)


threading.Thread(target=_memory_monitor_loop, daemon=True).start()


class RuntimeState:
    """Presentation-layer runtime bridge exposing bot cache/state to dashboard."""

    def __init__(self):
        self.bot = None
        self.gateway_reconnects = 0
        self.last_disconnect_at = 0
        self.shard_ready = {}

    def set_bot(self, bot):
        """Attach current bot instance reference."""
        self.bot = bot

    def get_guild(self, guild_id: int):
        """Return cached guild object from bot runtime if available."""
        if not self.bot:
            return None
        return self.bot.get_guild(guild_id)

    def latency_ms(self):
        """Return gateway latency in milliseconds for health endpoints."""
        if not self.bot:
            return 0
        return int((self.bot.latency or 0.0) * 1000)


RUNTIME = RuntimeState()


def set_module_health_provider(provider):
    global MODULE_HEALTH_PROVIDER
    MODULE_HEALTH_PROVIDER = provider


def set_scheduler_provider(provider):
    global SCHEDULER_PROVIDER
    SCHEDULER_PROVIDER = provider


def validate_critical_dependencies():
    if not CONFIG_PATH.exists():
        raise RuntimeError(f"config_missing path={CONFIG_PATH}")
    if not DB_PATH.parent.exists():
        raise RuntimeError(f"db_parent_missing path={DB_PATH.parent}")
    if not (BASE_DIR / "templates").exists():
        raise RuntimeError("templates_missing")
    if not (BASE_DIR / "static").exists():
        raise RuntimeError("static_missing")
    if not SECRET_KEY:
        raise RuntimeError("secret_key_missing")
    if SUPPORT_REPOSITORY is None or SUPPORT_SERVICE is None:
        raise RuntimeError("support_services_missing")
    if not CLIENT_ID or not CLIENT_SECRET or not REDIRECT_URI:
        LOGGER.warning("oauth_env_incomplete")


def run_async(coro):
    try:
        return asyncio.run(coro)
    except RuntimeError:
        # Fallback for edge-cases where a loop is already active in current thread.
        out = {}

        def _runner():
            try:
                out["value"] = asyncio.run(coro)
            except Exception as exc:
                out["error"] = exc

        t = threading.Thread(target=_runner, daemon=True)
        t.start()
        t.join(timeout=10)
        if t.is_alive():
            LOGGER.warning("run_async_fallback_timeout")
            return None
        if "error" in out:
            LOGGER.warning("run_async_fallback_failed", exc_info=out["error"])
            return None
        return out.get("value")


def run_on_bot_loop(coro, timeout_sec: float = 12):
    bot = RUNTIME.bot
    loop = getattr(bot, "loop", None) if bot else None
    if loop and loop.is_running():
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        try:
            return fut.result(timeout=timeout_sec)
        except Exception:
            LOGGER.warning("run_on_bot_loop_failed timeout_sec=%s", timeout_sec, exc_info=True)
            raise
    return run_async(coro)


def can_manage(guild_obj: dict) -> bool:
    try:
        perms = int(guild_obj.get("permissions", "0"))
        return (perms & MANAGE_GUILD) == MANAGE_GUILD
    except Exception:
        return False


def _manageable_guilds_from_payload(guilds):
    manageable = []
    for g in guilds:
        if can_manage(g):
            manageable.append({"id": str(g.get("id", "")), "name": g.get("name", "Unknown")})
    return manageable


def _runtime_guild_rows():
    rows = []
    if not RUNTIME.bot:
        return rows
    for g in sorted(RUNTIME.bot.guilds, key=lambda x: x.name.lower()):
        rows.append({"id": str(g.id), "name": g.name})
    return rows


def is_support_user_id(user_id: str) -> bool:
    uid = str(user_id or "").strip()
    if not uid or (OWNER_ID and uid == OWNER_ID):
        return False
    return uid in SUPPORT_ADMIN_IDS


def refresh_session_guilds(force: bool = False) -> bool:
    token = str(session.get("access_token", "")).strip()
    if not token:
        return False
    now = int(time.time())
    refresh_after = int(session.get("guilds_refresh_after", 0) or 0)
    if not force and now < refresh_after and isinstance(session.get("guilds"), list):
        return True
    user_id = str(session.get("user", {}).get("id", "")).strip()
    if is_support_user_id(user_id):
        support_guilds = _runtime_guild_rows()
        session["guilds"] = support_guilds
        session["allowed_guild_ids"] = [x["id"] for x in support_guilds]
        session["guilds_refreshed_at"] = now
        session["guilds_refresh_after"] = now + GUILDS_REFRESH_TTL_SEC
        session["is_support_admin"] = True
        return True
    guilds = discord_api("/users/@me/guilds", token)
    if guilds is None:
        return False
    manageable = _manageable_guilds_from_payload(guilds)
    session["guilds"] = manageable
    session["allowed_guild_ids"] = [x["id"] for x in manageable]
    session["guilds_refreshed_at"] = now
    session["guilds_refresh_after"] = now + GUILDS_REFRESH_TTL_SEC
    return True


def clean_hex(value: str) -> str:
    v = (value or "").strip().lstrip("#")
    if len(v) == 6 and all(c in "0123456789abcdefABCDEF" for c in v):
        return "#" + v.upper()
    return "#5865F2"


def boolv(data, key, default=False):
    if key not in data:
        return bool(default)
    value = data.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"1", "true", "yes", "on"}:
            return True
        if v in {"0", "false", "no", "off", ""}:
            return False
    return bool(value)


def intv(data, key, default, min_v, max_v):
    try:
        v = int(data.get(key, default))
    except Exception:
        v = int(default)
    return max(min_v, min(max_v, v))


def listv(data, key):
    v = data.get(key, [])
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        return [x.strip() for x in v.split(",") if x.strip()]
    return []


def _allowed_origin(origin: str) -> bool:
    if not origin:
        return True
    o = str(origin).strip().lower().rstrip("/")
    if not ALLOWED_ORIGINS:
        return True
    return o in ALLOWED_ORIGINS


def _safe_text(value: str, max_len: int, pattern: re.Pattern[str] | None = None) -> str:
    txt = sanitize_text(value, max_len)
    if not txt:
        return ""
    if pattern is None:
        pattern = SAFE_TEXT_RE
    if not pattern.fullmatch(txt):
        return ""
    return txt


def _json_body_or_error(max_keys: int = 120):
    if request.content_length and int(request.content_length) > MAX_JSON_BODY_BYTES:
        return None, (jsonify({"ok": False, "error": "payload_too_large"}), 413)
    if request.mimetype != "application/json":
        return None, (jsonify({"ok": False, "error": "invalid_content_type"}), 415)
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return None, (jsonify({"ok": False, "error": "invalid_json"}), 400)
    if len(data) > max_keys:
        return None, (jsonify({"ok": False, "error": "too_many_fields"}), 400)
    return data, None


def _audit_log(guild_id: str, action_type: str, actor_id: str, actor_name: str, target_id: str = "", metadata: dict | None = None):
    run_async(
        REPO.add_audit_event(
            guild_id=str(guild_id),
            actor_id=str(actor_id),
            actor_name=str(actor_name),
            action_type=str(action_type),
            target_id=str(target_id),
            metadata=metadata or {},
            created_at=int(time.time()),
        )
    )


def _init_support_layer():
    global SUPPORT_REPOSITORY, SUPPORT_SERVICE
    SUPPORT_REPOSITORY = SupportRepository(_db_fetchall_sync, _db_fetchone_sync, _db_execute_sync)
    SUPPORT_SERVICE = SupportService(
        repository=SUPPORT_REPOSITORY,
        realtime_service=REALTIME_SERVICE,
        audit_callback=_audit_log,
        send_webhook_callback=_support_send_webhook,
    )
    if SUPPORT_REPOSITORY is None or SUPPORT_SERVICE is None:
        raise RuntimeError("support_layer_init_failed")


_init_support_layer()


def get_oauth_url():
    state = os.urandom(24).hex()
    session["oauth_state"] = state
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": "identify guilds",
        "state": state,
    }
    return "https://discord.com/api/oauth2/authorize?" + urlencode(params)


def oauth_exchange(code: str):
    body = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        r = requests.post("https://discord.com/api/oauth2/token", data=body, headers=headers, timeout=20)
        if r.status_code != 200:
            return None
        return r.json()
    except requests.RequestException:
        LOGGER.warning("oauth_exchange_request_failed", exc_info=True)
        return None
    except ValueError:
        LOGGER.warning("oauth_exchange_json_failed", exc_info=True)
        return None


def discord_api(path: str, token: str):
    try:
        r = requests.get(f"https://discord.com/api{path}", headers={"Authorization": f"Bearer {token}"}, timeout=20)
        if r.status_code != 200:
            return None
        return r.json()
    except requests.RequestException:
        LOGGER.warning("discord_api_request_failed path=%s", path, exc_info=True)
        return None
    except ValueError:
        LOGGER.warning("discord_api_json_failed path=%s", path, exc_info=True)
        return None


def create_app():
    app = Flask(__name__, template_folder=str(BASE_DIR / "templates"), static_folder=None)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    if not SECRET_KEY:
        raise RuntimeError("SECRET_KEY missing")

    app.secret_key = SECRET_KEY
    app.config["MAX_CONTENT_LENGTH"] = MAX_JSON_BODY_BYTES
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SECURE"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

    @app.before_request
    def security_gate():
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "0.0.0.0").split(",")[0].strip()
        sid = session.get("user", {}).get("id", "anon")
        if not RATE_LIMITER.allow(ip, sid, request.path):
            return jsonify({"ok": False, "error": "rate_limited"}), 429
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            if not _allowed_origin(request.headers.get("Origin", "")):
                return jsonify({"ok": False, "error": "forbidden_origin"}), 403

    @app.after_request
    def security_headers(resp):
        resp.headers["X-Frame-Options"] = "DENY"
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        resp.headers["Permissions-Policy"] = "geolocation=(), camera=(), microphone=()"
        resp.headers["Content-Security-Policy"] = "default-src 'self' https:; script-src 'self' 'unsafe-inline' https:; style-src 'self' 'unsafe-inline' https:; frame-ancestors 'none'; object-src 'none'"
        return resp

    @app.errorhandler(Exception)
    def unhandled_error(exc):
        LOGGER.error("dashboard_unhandled_exception path=%s method=%s", request.path, request.method, exc_info=exc)
        if request.path.startswith("/dashboard/api/") or request.path.startswith("/api/"):
            return jsonify({"ok": False, "error": "internal_error"}), 500
        return "Internal server error", 500

    @app.context_processor
    def inject_globals():
        csrf_exp = int(session.get("csrf_expires", 0) or 0)
        if "csrf_token" not in session or int(time.time()) > csrf_exp:
            issue_csrf(session)
        uid = str(session.get("user", {}).get("id", "")).strip()
        owner_admin = bool(OWNER_ID and uid == OWNER_ID)
        support_admin = bool(is_support_user_id(uid))
        return {
            "csrf_token": session.get("csrf_token", ""),
            "feature_flags": GUILD_FEATURE_FLAGS,
            "is_support_admin": bool(session.get("is_support_admin", False) or support_admin),
            "is_owner_admin": bool(session.get("is_owner_admin", False) or owner_admin),
        }

    def logged_in():
        return bool(session.get("access_token"))

    def is_owner_user():
        uid = str(session.get("user", {}).get("id", ""))
        return bool(OWNER_ID and uid == OWNER_ID)

    def is_support_user():
        uid = str(session.get("user", {}).get("id", ""))
        return is_support_user_id(uid)

    def is_support_or_owner():
        return bool(is_owner_user() or is_support_user())

    def current_user_meta():
        u = session.get("user", {}) or {}
        uid = str(u.get("id", "")).strip()
        uname = sanitize_text(str(u.get("username", "user") or "user"), 64) or "user"
        return uid, uname

    def actor_role_for_guild(guild_id: str):
        if is_owner_user():
            return "owner"
        if is_support_user():
            return "support_admin"
        if str(guild_id) in allowed_guilds():
            return "server_admin"
        return "viewer"

    def can_access_support_portal(guild_id: str):
        gid = str(guild_id or "").strip()
        if not gid.isdigit():
            return False
        if can_edit_guild(gid):
            return True
        member_ids = {str(x) for x in session.get("member_guild_ids", [])}
        return gid in member_ids

    def allowed_guilds():
        return set(session.get("allowed_guild_ids", []))

    def can_edit_guild(guild_id: str):
        return PERMISSION_SERVICE.validate_dashboard_access(
            manageable_ids=allowed_guilds(),
            guild_id=str(guild_id),
            is_owner=is_owner_user(),
            is_support=is_support_user(),
        )

    async def _apply_verify_channel_lock(guild_id: str):
        if not str(guild_id).isdigit():
            return False, {"error": "bad_guild_id"}
        guild_obj = RUNTIME.get_guild(int(guild_id))
        if guild_obj is None:
            return False, {"error": "guild_unavailable"}
        me = guild_obj.me
        if me is None or not me.guild_permissions.manage_channels:
            return False, {"error": "bot_missing_manage_channels"}

        settings_cfg = (CONFIG_SERVICE.get_guild_config(str(guild_id)).get("modules", {}) or {}).get("settings", {}) or {}
        if not bool(settings_cfg.get("lock_enabled", False)):
            return False, {"error": "lock_disabled"}
        role_id = str(settings_cfg.get("verified_role_id") or settings_cfg.get("verify_role_id") or "").strip()
        verify_channel_id = str(settings_cfg.get("verify_channel_id") or settings_cfg.get("verify_panel_channel_id") or "").strip()
        lock_mode = str(settings_cfg.get("lock_mode", "all_except_verify")).strip()
        if lock_mode not in {"all_except_verify", "selected_categories"}:
            lock_mode = "all_except_verify"
        strict_hide_unverified = bool(settings_cfg.get("strict_hide_unverified", False))
        if not role_id.isdigit():
            return False, {"error": "verified_role_missing"}
        if not verify_channel_id.isdigit():
            return False, {"error": "verify_channel_missing"}
        verify_role = guild_obj.get_role(int(role_id))
        verify_channel = guild_obj.get_channel(int(verify_channel_id))
        if verify_role is None:
            return False, {"error": "verified_role_missing"}
        if verify_channel is None:
            return False, {"error": "verify_channel_missing"}

        selected_categories = {str(x) for x in settings_cfg.get("verify_lock_category_ids", []) if str(x).isdigit()}
        visible_ids = {str(x) for x in settings_cfg.get("verify_visible_channel_ids", []) if str(x).isdigit()}
        all_channels = list(guild_obj.channels)
        stale_role_ids = {str(x) for x in settings_cfg.get("verify_lock_last_applied_role_ids", []) if str(x).isdigit()}
        lock_role_ids = []
        changed = []
        failed = []
        for ch in all_channels:
            if not hasattr(ch, "set_permissions"):
                continue
            target_lock = False
            if str(ch.id) == str(verify_channel.id):
                target_lock = False
            elif lock_mode == "all_except_verify":
                target_lock = (str(ch.id) in visible_ids) if visible_ids else True
            else:
                cat_id = str(getattr(getattr(ch, "category", None), "id", ""))
                target_lock = bool(cat_id and cat_id in selected_categories)
            try:
                # Migrate old broken strict-lock data by removing stale role-level view denies/allows.
                for rid in stale_role_ids:
                    role_obj = guild_obj.get_role(int(rid))
                    if role_obj is None or role_obj >= me.top_role:
                        continue
                    try:
                        await ch.set_permissions(role_obj, view_channel=None, reason="verify lock migrate")
                    except Exception:
                        LOGGER.warning(
                            "verify_lock_migrate_role_clear_failed guild_id=%s channel_id=%s role_id=%s",
                            guild_id,
                            getattr(ch, "id", 0),
                            rid,
                            exc_info=True,
                        )
                if str(ch.id) == str(verify_channel.id):
                    await ch.set_permissions(
                        guild_obj.default_role,
                        view_channel=True,
                        read_message_history=True,
                        send_messages=False,
                        reason="verify lock apply",
                    )
                    await ch.set_permissions(
                        verify_role,
                        view_channel=False,
                        reason="verify lock apply",
                    )
                    changed.append(str(ch.id))
                    continue
                if target_lock:
                    await ch.set_permissions(
                        guild_obj.default_role,
                        view_channel=False,
                        reason="verify lock apply",
                    )
                    await ch.set_permissions(
                        verify_role,
                        view_channel=True,
                        reason="verify lock apply",
                    )
                    changed.append(str(ch.id))
                else:
                    # Cleanup old broad lock runs that exposed staff channels.
                    await ch.set_permissions(
                        verify_role,
                        view_channel=None,
                        reason="verify lock apply cleanup",
                    )
            except Exception:
                failed.append(str(ch.id))
                LOGGER.warning("verify_lock_apply_channel_failed guild_id=%s channel_id=%s", guild_id, getattr(ch, "id", 0), exc_info=True)

        settings_cfg["verify_lock_last_applied_channel_ids"] = changed
        settings_cfg["verify_lock_last_applied_role_ids"] = []
        CONFIG_SERVICE.update_guild_config(str(guild_id), "settings", settings_cfg)
        LOGGER.info(
            "verify_lock_apply guild_id=%s changed=%s failed=%s mode=%s",
            guild_id,
            len(changed),
            len(failed),
            lock_mode,
        )
        return True, {"changed": len(changed), "failed": failed, "mode": lock_mode}

    async def _restore_verify_channel_lock(guild_id: str):
        if not str(guild_id).isdigit():
            return False, {"error": "bad_guild_id"}
        guild_obj = RUNTIME.get_guild(int(guild_id))
        if guild_obj is None:
            return False, {"error": "guild_unavailable"}
        me = guild_obj.me
        if me is None or not me.guild_permissions.manage_channels:
            return False, {"error": "bot_missing_manage_channels"}

        settings_cfg = (CONFIG_SERVICE.get_guild_config(str(guild_id)).get("modules", {}) or {}).get("settings", {}) or {}
        role_id = str(settings_cfg.get("verified_role_id") or settings_cfg.get("verify_role_id") or "").strip()
        if not role_id.isdigit():
            return False, {"error": "verified_role_missing"}
        verify_role = guild_obj.get_role(int(role_id))
        if verify_role is None:
            return False, {"error": "verified_role_missing"}
        applied_role_ids = {str(x) for x in settings_cfg.get("verify_lock_last_applied_role_ids", []) if str(x).isdigit()}

        target_ids = {str(x) for x in settings_cfg.get("verify_lock_last_applied_channel_ids", []) if str(x).isdigit()}
        verify_channel_id = str(settings_cfg.get("verify_channel_id") or settings_cfg.get("verify_panel_channel_id") or "").strip()
        if verify_channel_id.isdigit():
            target_ids.add(verify_channel_id)
        if not target_ids:
            target_ids = {str(ch.id) for ch in guild_obj.channels if hasattr(ch, "set_permissions")}

        restored = 0
        failed = []
        for ch in guild_obj.channels:
            if str(getattr(ch, "id", "")) not in target_ids or not hasattr(ch, "set_permissions"):
                continue
            try:
                await ch.set_permissions(guild_obj.default_role, view_channel=None, send_messages=None, reason="verify lock restore")
                await ch.set_permissions(verify_role, view_channel=None, reason="verify lock restore")
                for rid in applied_role_ids:
                    role_obj = guild_obj.get_role(int(rid))
                    if role_obj is None or role_obj >= me.top_role:
                        continue
                    try:
                        await ch.set_permissions(role_obj, view_channel=None, reason="verify lock restore")
                    except Exception:
                        LOGGER.warning(
                            "verify_lock_restore_role_failed guild_id=%s channel_id=%s role_id=%s",
                            guild_id,
                            getattr(ch, "id", 0),
                            rid,
                            exc_info=True,
                        )
                restored += 1
            except Exception:
                failed.append(str(getattr(ch, "id", "")))
                LOGGER.warning(
                    "verify_lock_restore_channel_failed guild_id=%s channel_id=%s",
                    guild_id,
                    getattr(ch, "id", 0),
                    exc_info=True,
                )
        settings_cfg["verify_lock_last_applied_channel_ids"] = []
        settings_cfg["verify_lock_last_applied_role_ids"] = []
        CONFIG_SERVICE.update_guild_config(str(guild_id), "settings", settings_cfg)
        LOGGER.info("verify_lock_restore guild_id=%s restored=%s failed=%s", guild_id, restored, len(failed))
        return True, {"restored": restored, "failed": failed}

    async def _apply_verify_channel_visibility(guild_id: str):
        if not str(guild_id).isdigit():
            return False, "bad_guild_id"
        guild_obj = RUNTIME.get_guild(int(guild_id))
        if guild_obj is None:
            return False, "guild_unavailable"
        me = guild_obj.me
        if me is None or not me.guild_permissions.manage_channels or not me.guild_permissions.manage_roles:
            return False, "bot_missing_permissions"

        settings_cfg = (CONFIG_SERVICE.get_guild_config(str(guild_id)).get("modules", {}) or {}).get("settings", {}) or {}
        role_id = str(settings_cfg.get("verified_role_id") or settings_cfg.get("verify_role_id") or "").strip()
        if not role_id.isdigit():
            return False, "verify_role_missing"
        verify_role = guild_obj.get_role(int(role_id))
        if verify_role is None:
            return False, "verify_role_missing"

        visible_ids = {str(x) for x in settings_cfg.get("verify_visible_channel_ids", []) if str(x).isdigit()}
        changed = 0
        for ch in guild_obj.text_channels:
            if str(ch.id) in visible_ids:
                await ch.set_permissions(
                    verify_role,
                    view_channel=True,
                    send_messages=True,
                    read_message_history=True,
                    attach_files=True,
                    embed_links=True,
                    reason="apply verify channel visibility",
                )
                changed += 1
            else:
                await ch.set_permissions(verify_role, overwrite=None, reason="apply verify channel visibility")
                changed += 1
        return True, str(changed)

    async def _assign_verify_role(guild_id: str, user_id: str):
        if not str(guild_id).isdigit() or not str(user_id).isdigit():
            return False, "bad_id"
        guild_obj = RUNTIME.get_guild(int(guild_id))
        if guild_obj is None:
            return False, "guild_unavailable"
        me = guild_obj.me
        if me is None or not me.guild_permissions.manage_roles:
            return False, "bot_missing_manage_roles"

        member = guild_obj.get_member(int(user_id))
        if member is None:
            try:
                member = await guild_obj.fetch_member(int(user_id))
            except Exception:
                return False, "member_not_found"

        cfg = CONFIG_SERVICE.get_guild_config(str(guild_id)).get("modules", {})
        settings_cfg = cfg.get("settings", {}) if isinstance(cfg, dict) else {}
        if not bool(settings_cfg.get("enabled", True)):
            return False, "verify_disabled"
        if not bool(settings_cfg.get("verify_enabled", True)):
            return False, "verify_disabled"

        min_days = int(settings_cfg.get("verify_min_account_age_days", 7) or 7)
        min_days = max(0, min(365, min_days))
        age_days = max(0, int((discord.utils.utcnow() - member.created_at).total_seconds() // 86400))
        if age_days < min_days:
            return False, f"account_too_new:{min_days - age_days}"

        role_id = str(settings_cfg.get("verified_role_id") or settings_cfg.get("verify_role_id") or "").strip()
        if (not role_id) and isinstance(cfg, dict):
            role_id = str((cfg.get("autorole", {}) or {}).get("role_id", "")).strip()
        role = guild_obj.get_role(int(role_id)) if role_id.isdigit() else discord.utils.get(guild_obj.roles, name="Member")
        if role is None:
            return False, "verify_role_missing"
        if role >= me.top_role:
            return False, "verify_role_above_bot"

        if role in member.roles:
            return True, "already_verified"

        await member.add_roles(role, reason="Web verification passed")
        return True, "verified"

    @app.get("/dashboard/static/<path:filename>")
    def dashboard_static(filename):
        return send_from_directory(str(BASE_DIR / "static"), filename)

    @app.get("/dashboard")
    def dashboard_home():
        if not CLIENT_ID or not CLIENT_SECRET or not REDIRECT_URI:
            return "OAuth env missing", 500
        if not logged_in():
            return render_template("login.html")
        return redirect(url_for("dashboard_servers"))

    @app.get("/health")
    def health_public():
        return jsonify({"status": "ok", "ts": int(time.time())}), 200

    @app.get("/dashboard/login")
    def dashboard_login():
        session["post_login_target"] = "dashboard"
        return redirect(get_oauth_url())

    @app.get("/verify/login/<guild_id>")
    def verify_login(guild_id):
        if not str(guild_id).isdigit():
            return "Bad guild id", 400
        session["post_login_target"] = f"verify:{guild_id}"
        return redirect(get_oauth_url())

    @app.get("/support/login/<guild_id>")
    def support_login(guild_id):
        if not str(guild_id).isdigit():
            return "Bad guild id", 400
        session["post_login_target"] = f"support:{guild_id}"
        return redirect(get_oauth_url())

    @app.get("/support/login")
    def support_login_home():
        session["post_login_target"] = "support_home"
        return redirect(get_oauth_url())

    @app.get("/dashboard/callback")
    def dashboard_callback():
        code = request.args.get("code", "").strip()
        state = request.args.get("state", "").strip()
        if not code or not state or state != session.get("oauth_state", ""):
            session.clear()
            return "Invalid OAuth callback", 400

        token_data = oauth_exchange(code)
        if not token_data or "access_token" not in token_data:
            session.clear()
            return "OAuth failed", 400

        token = token_data["access_token"]
        user = discord_api("/users/@me", token)
        guilds = discord_api("/users/@me/guilds", token)
        if not user or guilds is None:
            session.clear()
            return "OAuth data failed", 400

        user_id = str(user.get("id", "")).strip()
        support_admin = is_support_user_id(user_id)
        manageable = _manageable_guilds_from_payload(guilds)
        member_guild_ids = [str(g.get("id", "")) for g in guilds if str(g.get("id", "")).isdigit()]
        if support_admin:
            runtime_rows = _runtime_guild_rows()
            if runtime_rows:
                manageable = runtime_rows
                member_guild_ids = [x["id"] for x in runtime_rows]
        now = int(time.time())

        session["access_token"] = token
        session["user"] = {"id": str(user.get("id", "")), "username": user.get("username", "user")}
        session["guilds"] = manageable
        session["allowed_guild_ids"] = [x["id"] for x in manageable]
        session["member_guild_ids"] = member_guild_ids
        session["is_support_admin"] = bool(support_admin)
        session["is_owner_admin"] = bool(OWNER_ID and user_id == OWNER_ID)
        session["guilds_refreshed_at"] = now
        session["guilds_refresh_after"] = now + GUILDS_REFRESH_TTL_SEC
        session.pop("oauth_state", None)
        issue_csrf(session)
        target = session.pop("post_login_target", "dashboard")
        if target == "admin":
            if not is_owner_user():
                return "Forbidden", 403
            return redirect(url_for("admin_servers"))
        if str(target).startswith("verify:"):
            gid = str(target).split(":", 1)[1].strip()
            if gid.isdigit():
                return redirect(url_for("verify_server", guild_id=gid))
        if str(target).startswith("support:"):
            gid = str(target).split(":", 1)[1].strip()
            if gid.isdigit():
                return redirect(url_for("support_portal", guild_id=gid))
        if target == "support_home":
            return redirect(url_for("support_home"))
        return redirect(url_for("dashboard_servers"))

    @app.get("/dashboard/logout")
    def dashboard_logout():
        session.clear()
        return redirect(url_for("dashboard_home"))

    @app.get("/verify/<guild_id>")
    def verify_server(guild_id):
        if not str(guild_id).isdigit():
            return "Bad guild id", 400
        guild = RUNTIME.get_guild(int(guild_id))
        guild_name = guild.name if guild else f"Server {guild_id}"

        if not logged_in():
            return render_template(
                "verify.html",
                title="Verify",
                guild_id=str(guild_id),
                guild_name=guild_name,
                state="login_required",
            )

        user_id = str(session.get("user", {}).get("id", ""))
        if not user_id.isdigit():
            session.clear()
            return redirect(url_for("verify_login", guild_id=str(guild_id)))

        ok = False
        status = "verify_failed"
        try:
            ok, status = run_on_bot_loop(_assign_verify_role(str(guild_id), user_id), timeout_sec=25)
        except Exception:
            LOGGER.warning("verify_assign_role_failed guild_id=%s user_id=%s", guild_id, user_id, exc_info=True)
            ok, status = False, "verify_failed"

        state = "success" if ok else "failed"
        message = "Verification successful. You can now return to Discord."
        if status == "already_verified":
            state = "success"
            message = "You are already verified on this server."
        elif status.startswith("account_too_new:"):
            left = status.split(":", 1)[1]
            state = "failed"
            message = f"Your Discord account is too new for verification. Please try again in {left} day(s)."
        elif status == "member_not_found":
            message = "Join the server first, then run verification again."
        elif status == "bot_missing_manage_roles":
            message = "Verification is temporarily unavailable. Please contact staff."
        elif status in {"verify_role_missing", "verify_role_above_bot", "verify_disabled", "guild_unavailable"}:
            message = "Verification is not configured correctly yet. Please contact staff."

        return render_template(
            "verify.html",
            title="Verify",
            guild_id=str(guild_id),
            guild_name=guild_name,
            state=state,
            message=message,
            user=session.get("user", {}),
        )

    @app.get("/support/<guild_id>")
    def support_portal(guild_id):
        if not str(guild_id).isdigit():
            return "Bad guild id", 400
        guild = RUNTIME.get_guild(int(guild_id))
        guild_name = guild.name if guild else f"Server {guild_id}"
        if not logged_in():
            return render_template(
                "support_portal.html",
                title="Support",
                guild_id=str(guild_id),
                guild_name=guild_name,
                state="login_required",
            )
        if not can_access_support_portal(guild_id):
            return render_template(
                "support_portal.html",
                title="Support",
                guild_id=str(guild_id),
                guild_name=guild_name,
                state="forbidden",
                user=session.get("user", {}),
            ), 403
        return render_template(
            "support_portal.html",
            title="Support",
            guild_id=str(guild_id),
            guild_name=guild_name,
            state="ready",
            user=session.get("user", {}),
        )

    @app.get("/support")
    def support_home():
        if not logged_in():
            return render_template(
                "login.html",
                login_title="Support",
                login_subtitle="Sign in to open support cases",
                login_path="/support/login",
            )
        refresh_session_guilds(force=False)
        runtime_map = {str(x["id"]): x["name"] for x in _runtime_guild_rows()}
        member_ids = [str(x) for x in session.get("member_guild_ids", []) if str(x).isdigit()]
        guild_rows = []
        for gid in member_ids:
            guild_rows.append({"id": gid, "name": runtime_map.get(gid, f"Server {gid}")})
        guild_rows.sort(key=lambda x: str(x["name"]).lower())
        return render_template("support_home.html", user=session.get("user", {}), guilds=guild_rows)

    @app.get("/admin")
    def admin_home():
        if not CLIENT_ID or not CLIENT_SECRET or not REDIRECT_URI:
            return "OAuth env missing", 500
        if not logged_in():
            return render_template(
                "login.html",
                login_title="Bot Admin",
                login_subtitle="Owner-only Discord OAuth sign-in",
                login_path="/admin/login",
            )
        if not is_owner_user():
            return "Forbidden", 403
        return redirect(url_for("admin_servers"))

    @app.get("/admin/login")
    def admin_login():
        session["post_login_target"] = "admin"
        return redirect(get_oauth_url())

    @app.get("/admin/servers")
    def admin_servers():
        if not logged_in():
            return redirect(url_for("admin_home"))
        if not is_owner_user():
            return "Forbidden", 403

        guild_rows = []
        if RUNTIME.bot:
            for g in sorted(RUNTIME.bot.guilds, key=lambda x: x.name.lower()):
                guild_rows.append(
                    {
                        "id": str(g.id),
                        "name": g.name,
                        "member_count": int(g.member_count or len(g.members)),
                        "channels": int(len(g.channels)),
                        "roles": int(len(g.roles)),
                    }
                )

        bot_stats = {
            "guilds": int(len(guild_rows)),
            "users_cached": int(sum(x["member_count"] for x in guild_rows)),
            "latency_ms": int(RUNTIME.latency_ms()),
        }
        return render_template("admin_servers.html", user=session.get("user", {}), guilds=guild_rows, bot_stats=bot_stats)

    @app.get("/admin/guild/<guild_id>")
    def admin_guild(guild_id):
        if not logged_in():
            return redirect(url_for("admin_home"))
        if not is_owner_user():
            return "Forbidden", 403

        try:
            gid = int(guild_id)
        except Exception:
            return "Bad guild id", 400

        guild = RUNTIME.get_guild(gid)
        if guild is None:
            return "Guild not found in bot cache", 404

        members = []
        for m in sorted(guild.members, key=lambda x: (x.bot, str(x).lower())):
            members.append(
                {
                    "id": str(m.id),
                    "username": str(m),
                    "display_name": m.display_name,
                    "bot": bool(m.bot),
                    "status": str(getattr(m, "status", "offline")),
                    "top_role": str(getattr(m.top_role, "name", "@everyone")),
                }
            )
        return render_template(
            "admin_guild.html",
            guild={
                "id": str(guild.id),
                "name": guild.name,
                "member_count": int(guild.member_count or len(guild.members)),
                "channels": int(len(guild.channels)),
                "roles": int(len(guild.roles)),
            },
            members=members,
            user=session.get("user", {}),
        )

    @app.get("/dashboard/servers")
    def dashboard_servers():
        if not logged_in():
            return redirect(url_for("dashboard_home"))
        refresh_session_guilds(force=False)
        return render_template("servers.html", user=session.get("user", {}), guilds=session.get("guilds", []))

    @app.get("/dashboard/support")
    def dashboard_support():
        if not logged_in():
            return redirect(url_for("dashboard_home"))
        if not is_support_or_owner():
            return "Forbidden", 403
        refresh_session_guilds(force=False)
        return render_template("support.html", user=session.get("user", {}))

    @app.get("/dashboard/guild/<guild_id>")
    def dashboard_guild(guild_id):
        if not logged_in():
            return redirect(url_for("dashboard_home"))
        refresh_session_guilds(force=False)
        if not can_edit_guild(guild_id):
            return "Forbidden", 403

        cfg = CONFIG_SERVICE.get_guild_config(guild_id)
        guild = RUNTIME.get_guild(int(guild_id))

        member_count = 0
        online_count = 0
        boost_level = 0
        role_options = []
        channel_options = []
        category_options = []

        if guild:
            member_count = guild.member_count or len(guild.members)
            online_count = len([m for m in guild.members if str(getattr(m, "status", "offline")) != "offline"])
            boost_level = int(guild.premium_tier or 0)
            role_options = [
                {"id": str(r.id), "name": r.name}
                for r in sorted(guild.roles, key=lambda x: x.position, reverse=True)
                if not r.is_default() and not r.is_bot_managed()
            ]
            channel_options = [{"id": str(c.id), "name": c.name} for c in guild.text_channels]
            category_options = [{"id": str(c.id), "name": c.name} for c in guild.categories]

        stats = {
            "member_count": member_count,
            "online_count": online_count,
            "boost_level": boost_level,
            "latency_ms": RUNTIME.latency_ms(),
        }

        return render_template(
            "dashboard.html",
            guild_id=str(guild_id),
            cfg=cfg,
            stats=stats,
            role_options=role_options,
            channel_options=channel_options,
            category_options=category_options,
            support_privileged=bool(can_edit_guild(guild_id) or is_support_or_owner()),
        )

    @app.get("/dashboard/api/stats/<guild_id>")
    def dashboard_stats(guild_id):
        if not logged_in() or not can_edit_guild(guild_id):
            return jsonify({"error": "forbidden"}), 403

        guild = RUNTIME.get_guild(int(guild_id))
        if not guild:
            return jsonify({"member_count": 0, "online_count": 0, "boost_level": 0, "latency_ms": RUNTIME.latency_ms()})

        online_count = len([m for m in guild.members if str(getattr(m, "status", "offline")) != "offline"])
        return jsonify({
            "member_count": guild.member_count or len(guild.members),
            "online_count": online_count,
            "boost_level": int(guild.premium_tier or 0),
            "latency_ms": RUNTIME.latency_ms(),
        })

    @app.get("/dashboard/api/analytics/<guild_id>")
    def dashboard_analytics(guild_id):
        if not logged_in() or not can_edit_guild(guild_id):
            return jsonify({"error": "forbidden"}), 403
        mode = request.args.get("range", "daily")
        if mode == "weekly":
            data = run_async(METRICS.weekly(str(guild_id))) or {}
        else:
            data = run_async(METRICS.daily(str(guild_id))) or {}
        return jsonify({"range": mode, "metrics": data})

    def _get_case_or_none(case_id: str):
        if not str(case_id).isdigit():
            return None
        if SUPPORT_SERVICE is None:
            return None
        return SUPPORT_SERVICE.get_case(int(case_id))

    def _can_access_case(case_row) -> bool:
        if not case_row:
            return False
        gid = str(case_row.get("guild_id", ""))
        uid, _ = current_user_meta()
        return bool(
            can_edit_guild(gid)
            or is_support_or_owner()
            or (can_access_support_portal(gid) and str(case_row.get("created_by_id", "")) == uid)
        )

    @app.get("/dashboard/api/support/cases")
    def dashboard_support_cases():
        if not logged_in():
            return jsonify({"error": "unauthorized"}), 401
        guild_id = sanitize_id(request.args.get("guild_id", ""))
        status = sanitize_text(request.args.get("status", ""), 24).lower()
        priority = sanitize_text(request.args.get("priority", ""), 24).lower()
        q = sanitize_text(request.args.get("q", ""), 120).strip().lower()
        uid, _ = current_user_meta()
        params = []
        where = []
        if status in {"open", "in_progress", "resolved", "closed"}:
            where.append("status=?")
            params.append(status)
        if priority in {"low", "normal", "high", "urgent"}:
            where.append("priority=?")
            params.append(priority)
        if guild_id:
            if not can_access_support_portal(guild_id):
                return jsonify({"error": "forbidden"}), 403
            where.append("guild_id=?")
            params.append(str(guild_id))
            if not (can_edit_guild(guild_id) or is_support_or_owner()):
                where.append("created_by_id=?")
                params.append(uid)
        elif not is_support_or_owner():
            return jsonify({"error": "guild_id_required"}), 400
        sql = """
            SELECT 1
        """
        where_sql = ""
        if where:
            where_sql = " WHERE " + " AND ".join(where)
        rows = SUPPORT_SERVICE.list_cases(where_sql, tuple(params), q) if SUPPORT_SERVICE else []
        now = int(time.time())
        for row in rows:
            waiting_on = "support" if str(row.get("last_actor_role", "requester")) == "requester" else "requester"
            wait_sec = max(0, now - int(row.get("last_message_at", 0) or now))
            row["waiting_on"] = waiting_on
            row["wait_sec"] = wait_sec
            row["sla_breached"] = bool(
                row.get("status") in {"open", "in_progress"}
                and waiting_on == "support"
                and (
                    (row.get("priority") == "urgent" and wait_sec > 15 * 60)
                    or (row.get("priority") == "high" and wait_sec > 60 * 60)
                    or (row.get("priority") == "normal" and wait_sec > 4 * 60 * 60)
                    or (row.get("priority") == "low" and wait_sec > 12 * 60 * 60)
                )
            )
        return jsonify({"cases": rows})

    @app.post("/dashboard/api/support/cases")
    def dashboard_support_create_case():
        if not logged_in():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        data, err = _json_body_or_error(max_keys=20)
        if err:
            return err
        token = request.headers.get("X-CSRF-Token", "") or str(data.get("csrf_token", ""))
        if not validate_csrf(session, token):
            new_token = issue_csrf(session)
            return jsonify({"ok": False, "error": "csrf_failed", "csrf_token": new_token}), 403
        guild_id = sanitize_id(data.get("guild_id", ""))
        if not guild_id or not can_access_support_portal(guild_id):
            return jsonify({"ok": False, "error": "forbidden"}), 403
        subject = _safe_text(data.get("subject", ""), 140)
        body = _safe_text(data.get("message", ""), 3000)
        priority = sanitize_text(data.get("priority", "normal"), 24).lower()
        if priority not in {"low", "normal", "high", "urgent"}:
            priority = "normal"
        if not subject or not body:
            return jsonify({"ok": False, "error": "subject_and_message_required"}), 400
        uid, uname = current_user_meta()
        now = int(time.time())
        guild_obj = RUNTIME.get_guild(int(guild_id)) if str(guild_id).isdigit() else None
        guild_name = guild_obj.name if guild_obj else f"Guild {guild_id}"
        case_id = SUPPORT_SERVICE.create_case(
            guild_id=str(guild_id),
            guild_name=guild_name,
            created_by_id=uid,
            created_by_name=uname,
            subject=subject,
            priority=priority,
            body=body,
            actor_role=actor_role_for_guild(guild_id),
        )
        return jsonify({"ok": True, "case_id": int(case_id)})

    @app.get("/dashboard/api/support/cases/<case_id>/messages")
    def dashboard_support_case_messages(case_id):
        if not logged_in():
            return jsonify({"error": "unauthorized"}), 401
        case_row = _get_case_or_none(case_id)
        if case_row is None:
            return jsonify({"error": "not_found"}), 404
        if not _can_access_case(case_row):
            return jsonify({"error": "forbidden"}), 403
        gid = str(case_row.get("guild_id", ""))
        privileged = bool(can_edit_guild(gid) or is_support_or_owner())
        msgs = SUPPORT_SERVICE.list_messages(int(case_id), privileged) if SUPPORT_SERVICE else []
        return jsonify({"case": case_row, "messages": msgs})

    @app.post("/dashboard/api/support/cases/<case_id>/messages")
    def dashboard_support_case_add_message(case_id):
        if not logged_in():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        case_row = _get_case_or_none(case_id)
        if case_row is None:
            return jsonify({"ok": False, "error": "not_found"}), 404
        if not _can_access_case(case_row):
            return jsonify({"ok": False, "error": "forbidden"}), 403
        data, err = _json_body_or_error(max_keys=20)
        if err:
            return err
        token = request.headers.get("X-CSRF-Token", "") or str(data.get("csrf_token", ""))
        if not validate_csrf(session, token):
            new_token = issue_csrf(session)
            return jsonify({"ok": False, "error": "csrf_failed", "csrf_token": new_token}), 403
        body = _safe_text(data.get("message", ""), 3000)
        if not body:
            return jsonify({"ok": False, "error": "message_required"}), 400
        uid, uname = current_user_meta()
        gid = str(case_row.get("guild_id", ""))
        role = actor_role_for_guild(gid)
        privileged = bool(can_edit_guild(gid) or is_support_or_owner())
        visibility = "internal" if (sanitize_text(data.get("visibility", "public"), 16) == "internal" and privileged) else "public"
        if SUPPORT_SERVICE is None:
            return jsonify({"ok": False, "error": "support_unavailable"}), 503
        SUPPORT_SERVICE.add_message(case_row, uid, uname, role, body, visibility)
        _support_maybe_send_sla_alert(int(case_id))
        return jsonify({"ok": True})

    @app.post("/dashboard/api/support/cases/<case_id>/assign")
    def dashboard_support_case_assign(case_id):
        if not logged_in():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        if not is_support_or_owner():
            return jsonify({"ok": False, "error": "forbidden"}), 403
        case_row = _get_case_or_none(case_id)
        if case_row is None:
            return jsonify({"ok": False, "error": "not_found"}), 404
        data, err = _json_body_or_error(max_keys=10)
        if err:
            return err
        token = request.headers.get("X-CSRF-Token", "") or str(data.get("csrf_token", ""))
        if not validate_csrf(session, token):
            new_token = issue_csrf(session)
            return jsonify({"ok": False, "error": "csrf_failed", "csrf_token": new_token}), 403
        action = sanitize_text(data.get("action", "assign_me"), 24).lower()
        uid, uname = current_user_meta()
        if SUPPORT_SERVICE is None:
            return jsonify({"ok": False, "error": "support_unavailable"}), 503
        SUPPORT_SERVICE.assign_case(case_row, action, uid, uname)
        _support_maybe_send_sla_alert(int(case_id))
        return jsonify({"ok": True})

    @app.post("/dashboard/api/support/cases/<case_id>/status")
    def dashboard_support_case_status(case_id):
        if not logged_in():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        case_row = _get_case_or_none(case_id)
        if case_row is None:
            return jsonify({"ok": False, "error": "not_found"}), 404
        if not _can_access_case(case_row):
            return jsonify({"ok": False, "error": "forbidden"}), 403
        data, err = _json_body_or_error(max_keys=10)
        if err:
            return err
        token = request.headers.get("X-CSRF-Token", "") or str(data.get("csrf_token", ""))
        if not validate_csrf(session, token):
            new_token = issue_csrf(session)
            return jsonify({"ok": False, "error": "csrf_failed", "csrf_token": new_token}), 403
        status = sanitize_text(data.get("status", ""), 24).lower()
        if status not in {"open", "in_progress", "resolved", "closed"}:
            return jsonify({"ok": False, "error": "bad_status"}), 400
        uid, _ = current_user_meta()
        case_creator = str(case_row.get("created_by_id", "")) == uid
        if status in {"in_progress", "closed"} and not is_support_or_owner():
            return jsonify({"ok": False, "error": "forbidden"}), 403
        if status in {"resolved", "open"} and not (is_support_or_owner() or case_creator):
            return jsonify({"ok": False, "error": "forbidden"}), 403
        if SUPPORT_SERVICE is None:
            return jsonify({"ok": False, "error": "support_unavailable"}), 503
        SUPPORT_SERVICE.update_status(case_row, status, uid, str(session.get("user", {}).get("username", "user")))
        _support_maybe_send_sla_alert(int(case_id))
        return jsonify({"ok": True})

    @app.get("/dashboard/api/ws-token/<guild_id>")
    def dashboard_ws_token(guild_id):
        gid = str(guild_id)
        if not logged_in():
            return jsonify({"error": "forbidden"}), 403
        if gid == "support:global":
            if not is_support_or_owner():
                return jsonify({"error": "forbidden"}), 403
        elif not (can_edit_guild(gid) or can_access_support_portal(gid)):
            return jsonify({"error": "forbidden"}), 403
        token = REALTIME_SERVICE.issue_ws_token(session.get("user", {}).get("id", "0"), gid, ttl_sec=120)
        scheme = "wss" if request.is_secure else "ws"
        ws_url = f"{scheme}://{request.host}/dashboard/ws?guild={gid}&token={token}"
        return jsonify({"token": token, "ws_url": ws_url})

    @app.get("/dashboard/api/diagnostics/<guild_id>")
    def dashboard_diagnostics(guild_id):
        if not logged_in() or not can_edit_guild(guild_id):
            return jsonify({"error": "forbidden"}), 403
        if not str(guild_id).isdigit():
            return jsonify({"error": "bad_guild_id"}), 400
        guild = RUNTIME.get_guild(int(guild_id))
        if guild is None:
            return jsonify(
                {
                    "guild_in_bot_cache": False,
                    "guild_id": str(guild_id),
                    "checks": {
                        "bot_member_present": False,
                        "manage_roles": False,
                        "manage_channels": False,
                    },
                    "issues": ["guild_unavailable_to_bot"],
                }
            )

        settings_cfg = (CONFIG_SERVICE.get_guild_config(str(guild_id)).get("modules", {}) or {}).get("settings", {}) or {}
        me = guild.me
        perms = getattr(me, "guild_permissions", None)
        verify_role_id = str(settings_cfg.get("verified_role_id") or settings_cfg.get("verify_role_id") or "").strip()
        verify_channel_id = str(settings_cfg.get("verify_channel_id") or settings_cfg.get("verify_panel_channel_id") or "").strip()
        verify_role_exists = bool(verify_role_id.isdigit() and guild.get_role(int(verify_role_id)))
        verify_channel_exists = bool(verify_channel_id.isdigit() and guild.get_channel(int(verify_channel_id)))

        checks = {
            "bot_member_present": bool(me),
            "manage_roles": bool(perms and perms.manage_roles),
            "manage_channels": bool(perms and perms.manage_channels),
            "verify_enabled": bool(settings_cfg.get("verify_enabled", True)),
            "lock_enabled": bool(settings_cfg.get("lock_enabled", False)),
            "verified_role_configured": bool(verify_role_id.isdigit()),
            "verify_channel_configured": bool(verify_channel_id.isdigit()),
            "verified_role_exists": verify_role_exists,
            "verify_channel_exists": verify_channel_exists,
        }
        issues = [k for k, ok in checks.items() if not ok and k in {
            "bot_member_present",
            "manage_roles",
            "manage_channels",
            "verified_role_configured",
            "verify_channel_configured",
            "verified_role_exists",
            "verify_channel_exists",
        }]
        return jsonify(
            {
                "guild_in_bot_cache": True,
                "guild_id": str(guild.id),
                "guild_name": guild.name,
                "checks": checks,
                "issues": issues,
            }
        )

    @app.get("/dashboard/realtime/<guild_id>")
    def dashboard_realtime(guild_id):
        if not logged_in() or not can_edit_guild(guild_id):
            return jsonify({"error": "forbidden"}), 403

        q = REALTIME_BUS.subscribe(str(guild_id))

        def stream():
            try:
                while True:
                    try:
                        payload = q.get(timeout=25)
                        yield f"data: {payload}\n\n"
                    except Empty:
                        ping = json.dumps({"type": "ping", "t": int(time.time())})
                        yield f"data: {ping}\n\n"
            finally:
                REALTIME_BUS.unsubscribe(str(guild_id), q)

        return Response(stream(), mimetype="text/event-stream")

    @app.get("/dashboard/health")
    def dashboard_health():
        if not logged_in():
            return jsonify({"status": "ok"}), 200
        mem = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=0.1)
        proc_mem = PROCESS.memory_info()
        shard_status = {}
        if RUNTIME.bot and getattr(RUNTIME.bot, "shards", None):
            for sid, shard in RUNTIME.bot.shards.items():
                try:
                    shard_status[str(sid)] = {
                        "latency_ms": int((getattr(shard, "latency", 0.0) or 0.0) * 1000),
                        "closed": bool(shard.is_closed()),
                    }
                except Exception:
                    shard_status[str(sid)] = {"latency_ms": 0, "closed": True}

        scheduler_queue_length = 0
        if SCHEDULER_PROVIDER is not None:
            try:
                scheduler_queue_length = int(SCHEDULER_PROVIDER.queue_length())
            except Exception:
                LOGGER.warning("health_scheduler_queue_failed", exc_info=True)

        module_health = {}
        if callable(MODULE_HEALTH_PROVIDER):
            try:
                module_health = MODULE_HEALTH_PROVIDER()
            except Exception:
                LOGGER.warning("health_module_provider_failed", exc_info=True)

        memory_growth_5m_mb = 0
        if len(MEMORY_SAMPLES) >= 2:
            t0, rss0 = MEMORY_SAMPLES[-2]
            t1, rss1 = MEMORY_SAMPLES[-1]
            if (t1 - t0) >= 240:
                memory_growth_5m_mb = int((rss1 - rss0) / (1024 * 1024))

        return jsonify(
            {
                "status": "ok",
                "cpu_percent": cpu,
                "memory_percent": mem.percent,
                "memory_used_mb": int(mem.used / (1024 * 1024)),
                "process_rss_mb": int(proc_mem.rss / (1024 * 1024)),
                "process_threads": int(PROCESS.num_threads()),
                "process_open_fds": int(PROCESS.num_fds()) if hasattr(PROCESS, "num_fds") else 0,
                "uptime_sec": int(time.time()) - STARTED_AT,
                "memory_growth_5m_mb": memory_growth_5m_mb,
                "latency_ms": RUNTIME.latency_ms(),
                "gateway_connected": bool(getattr(RUNTIME.bot, "is_ready", lambda: False)()),
                "gateway_reconnects": int(RUNTIME.gateway_reconnects),
                "last_disconnect_at": int(RUNTIME.last_disconnect_at),
                "shard_health": dict(RUNTIME.shard_ready),
                "shard_status": shard_status,
                "scheduler_queue_length": scheduler_queue_length,
                "module_health": module_health,
            }
        )

    @app.get("/dashboard/api/modules")
    def dashboard_modules():
        if not logged_in():
            return jsonify({"error": "unauthorized"}), 401
        return jsonify(
            {
                "modules": [
                    "welcome",
                    "autorole",
                    "automod",
                    "logging",
                    "custom_commands",
                    "commands",
                    "moderation_tools",
                    "reaction_roles",
                    "leveling_system",
                    "webhooks",
                    "integrations",
                    "premium",
                    "settings",
                ]
            }
        )

    @app.post("/api/verify/apply-lock")
    @app.post("/dashboard/api/verify/apply-lock")
    def api_verify_apply_lock():
        if not logged_in():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        data, err = _json_body_or_error(max_keys=40)
        if err:
            return err
        guild_id = sanitize_id(data.get("guild_id", ""))
        if not guild_id or not can_edit_guild(guild_id):
            return jsonify({"ok": False, "error": "forbidden"}), 403
        token = request.headers.get("X-CSRF-Token", "") or str(data.get("csrf_token", ""))
        if not validate_csrf(session, token):
            new_token = issue_csrf(session)
            return jsonify({"ok": False, "error": "csrf_failed", "csrf_token": new_token}), 403
        try:
            if bool(data):
                cfg = CONFIG_SERVICE.get_guild_config(str(guild_id))
                modules = cfg.setdefault("modules", {})
                settings_cfg = modules.setdefault("settings", {})
                settings_cfg["lock_enabled"] = boolv(
                    data,
                    "lock_enabled",
                    bool(settings_cfg.get("lock_enabled", False)),
                )
                role_id = sanitize_id(data.get("verified_role_id", "")) or sanitize_id(data.get("verify_role_id", ""))
                if role_id:
                    settings_cfg["verified_role_id"] = role_id
                    settings_cfg["verify_role_id"] = role_id
                verify_channel_id = sanitize_id(data.get("verify_channel_id", "")) or sanitize_id(data.get("verify_panel_channel_id", ""))
                if verify_channel_id:
                    settings_cfg["verify_channel_id"] = verify_channel_id
                    settings_cfg["verify_panel_channel_id"] = verify_channel_id
                lock_mode = sanitize_text(data.get("lock_mode", settings_cfg.get("lock_mode", "all_except_verify")), 32)
                if lock_mode not in {"all_except_verify", "selected_categories"}:
                    lock_mode = "all_except_verify"
                settings_cfg["lock_mode"] = lock_mode
                settings_cfg["verify_lock_category_ids"] = [
                    sanitize_id(x) for x in listv(data, "verify_lock_category_ids") if sanitize_id(x)
                ][:100] or settings_cfg.get("verify_lock_category_ids", [])
                CONFIG_SERVICE.update_guild_config(str(guild_id), "settings", settings_cfg)
            ok, out = run_on_bot_loop(_apply_verify_channel_lock(guild_id), timeout_sec=90)
            if not ok:
                return jsonify({"ok": False, **out}), 400
            uid, uname = current_user_meta()
            _audit_log(guild_id, "verify_lock_applied", uid, uname, metadata={"mode": str(data.get("lock_mode", ""))[:32]})
            return jsonify({"ok": True, **out})
        except Exception:
            LOGGER.warning("verify_lock_apply_failed guild_id=%s", guild_id, exc_info=True)
            return jsonify({"ok": False, "error": "apply_failed"}), 500

    @app.post("/api/verify/restore-lock")
    @app.post("/dashboard/api/verify/restore-lock")
    def api_verify_restore_lock():
        if not logged_in():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        data, err = _json_body_or_error(max_keys=12)
        if err:
            return err
        guild_id = sanitize_id(data.get("guild_id", ""))
        if not guild_id or not can_edit_guild(guild_id):
            return jsonify({"ok": False, "error": "forbidden"}), 403
        token = request.headers.get("X-CSRF-Token", "") or str(data.get("csrf_token", ""))
        if not validate_csrf(session, token):
            new_token = issue_csrf(session)
            return jsonify({"ok": False, "error": "csrf_failed", "csrf_token": new_token}), 403
        try:
            ok, out = run_on_bot_loop(_restore_verify_channel_lock(guild_id), timeout_sec=90)
            if not ok:
                return jsonify({"ok": False, **out}), 400
            uid, uname = current_user_meta()
            _audit_log(guild_id, "verify_lock_restored", uid, uname)
            return jsonify({"ok": True, **out})
        except Exception:
            LOGGER.warning("verify_lock_restore_failed guild_id=%s", guild_id, exc_info=True)
            return jsonify({"ok": False, "error": "restore_failed"}), 500

    @app.post("/dashboard/save/<guild_id>/<module_name>")
    def dashboard_save(guild_id, module_name):
        if not logged_in():
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        if not can_edit_guild(guild_id):
            return jsonify({"ok": False, "error": "forbidden"}), 403
        data, err = _json_body_or_error(max_keys=300)
        if err:
            return err
        token = request.headers.get("X-CSRF-Token", "") or str(data.get("csrf_token", ""))
        if not validate_csrf(session, token):
            new_token = issue_csrf(session)
            return jsonify({"ok": False, "error": "csrf_failed", "csrf_token": new_token}), 403

        if module_name == "welcome":
            payload = {
                "enabled": boolv(data, "enabled", False),
                "channel_id": sanitize_id(data.get("channel_id", "")),
                "title": sanitize_text(data.get("title", welcome.DEFAULT["title"]), 256),
                "description": sanitize_text(data.get("description", welcome.DEFAULT["description"]), 3000),
                "color": clean_hex(str(data.get("color", welcome.DEFAULT["color"]))),
                "image": sanitize_text(data.get("image", ""), 500),
                "thumbnail": sanitize_text(data.get("thumbnail", ""), 500),
                "footer": sanitize_text(data.get("footer", ""), 200),
            }
        elif module_name == "autorole":
            payload = {
                "enabled": boolv(data, "enabled", False),
                "role_id": sanitize_id(data.get("role_id", "")),
            }
        elif module_name == "automod":
            words = data.get("banned_words", [])
            if isinstance(words, str):
                words = [x.strip() for x in words.replace("\n", ",").split(",") if x.strip()]
            if not isinstance(words, list):
                words = []
            allowed_domains = data.get("allowed_domains", [])
            if isinstance(allowed_domains, str):
                allowed_domains = [x.strip().lower() for x in allowed_domains.replace("\n", ",").split(",") if x.strip()]
            if not isinstance(allowed_domains, list):
                allowed_domains = []
            word_match_mode = sanitize_text(data.get("word_match_mode", "contains"), 24).lower()
            if word_match_mode not in {"contains", "whole_word"}:
                word_match_mode = "contains"
            payload = {
                "enabled": boolv(data, "enabled", False),
                "anti_invite": boolv(data, "anti_invite", True),
                "anti_links": boolv(data, "anti_links", False),
                "allowed_domains": [sanitize_text(x, 120).lower() for x in allowed_domains if sanitize_text(x, 120)][:100],
                "anti_spam": boolv(data, "anti_spam", True),
                "anti_mass_mentions": boolv(data, "anti_mass_mentions", False),
                "mention_threshold": intv(data, "mention_threshold", 5, 2, 15),
                "anti_caps": boolv(data, "anti_caps", False),
                "caps_threshold": intv(data, "caps_threshold", 80, 50, 100),
                "max_message_length": intv(data, "max_message_length", 0, 0, 4000),
                "block_attachments": boolv(data, "block_attachments", False),
                "max_attachment_size_mb": intv(data, "max_attachment_size_mb", 8, 1, 100),
                "exempt_role_ids": [sanitize_id(x) for x in listv(data, "exempt_role_ids") if sanitize_id(x)][:50],
                "exempt_channel_ids": [sanitize_id(x) for x in listv(data, "exempt_channel_ids") if sanitize_id(x)][:50],
                "spam_threshold": intv(data, "spam_threshold", 5, 2, 12),
                "spam_window_seconds": intv(data, "spam_window_seconds", 8, 3, 20),
                "banned_words": [sanitize_text(x, 48) for x in words if sanitize_text(x, 48)][:300],
                "word_match_mode": word_match_mode,
                "case_sensitive": boolv(data, "case_sensitive", False),
                "delete_message": boolv(data, "delete_message", True),
                "send_warning": boolv(data, "send_warning", True),
                "warning_delete_after_seconds": intv(data, "warning_delete_after_seconds", 6, 1, 60),
                "log_violations": boolv(data, "log_violations", True),
            }
        elif module_name == "logging":
            payload = {
                "enabled": boolv(data, "enabled", False),
                "channel_id": sanitize_id(data.get("channel_id", "")),
                "joins": boolv(data, "joins", True),
                "leaves": boolv(data, "leaves", True),
                "bans": boolv(data, "bans", True),
                "kicks": boolv(data, "kicks", True),
                "message_deletions": boolv(data, "message_deletions", True),
            }
        elif module_name == "custom_commands":
            commands_raw = sanitize_text(data.get("commands_raw", ""), 20000)
            commands = []
            for line in commands_raw.splitlines():
                row = [x.strip() for x in line.split("|")]
                if len(row) < 2:
                    continue
                name = sanitize_text(row[0].lower(), 32)
                if not name:
                    continue
                description = sanitize_text(row[1] if len(row) > 2 else f"Run {name}", 100)
                response = sanitize_text(row[-1], MAX_CUSTOM_RESPONSE_LEN)
                if not response:
                    continue
                commands.append({"name": name, "description": description, "response": response})
            payload = {"enabled": boolv(data, "enabled", False), "commands": commands[:MAX_CUSTOM_COMMANDS_PER_GUILD]}
        elif module_name == "commands":
            payload = {
                "enabled": boolv(data, "enabled", True),
                "custom_commands_enabled": boolv(data, "custom_commands_enabled", True),
                "custom_commands_max": intv(data, "custom_commands_max", 20, 1, MAX_CUSTOM_COMMANDS_PER_GUILD),
                "custom_response_ephemeral": boolv(data, "custom_response_ephemeral", False),
                "command_reloadconfig": boolv(data, "command_reloadconfig", True),
                "command_modules_health": boolv(data, "command_modules_health", True),
                "command_warn": boolv(data, "command_warn", True),
                "command_warnings": boolv(data, "command_warnings", True),
                "command_clearwarnings": boolv(data, "command_clearwarnings", True),
                "command_timeout": boolv(data, "command_timeout", True),
                "command_kick": boolv(data, "command_kick", False),
                "command_ban": boolv(data, "command_ban", False),
            }
        elif module_name == "moderation_tools":
            payload = {
                "enabled": boolv(data, "enabled", False),
                "warn_threshold": intv(data, "warn_threshold", 3, 1, 10),
                "timeout_minutes": intv(data, "timeout_minutes", 10, 1, 1440),
                "auto_kick": boolv(data, "auto_kick", False),
                "moderator_role_ids": [sanitize_id(x) for x in listv(data, "moderator_role_ids") if sanitize_id(x)][:50],
                "command_warn": boolv(data, "command_warn", True),
                "command_warnings": boolv(data, "command_warnings", True),
                "command_clearwarnings": boolv(data, "command_clearwarnings", True),
                "command_timeout": boolv(data, "command_timeout", True),
                "command_kick": boolv(data, "command_kick", False),
                "command_ban": boolv(data, "command_ban", False),
            }
        elif module_name == "reaction_roles":
            pairs_raw = sanitize_text(data.get("pairs_raw", ""), 4000)
            pairs = []
            for line in pairs_raw.splitlines():
                row = [x.strip() for x in line.split("|")]
                if len(row) != 2:
                    continue
                emoji = sanitize_text(row[0], 64)
                role_id = sanitize_id(row[1])
                if emoji and role_id:
                    pairs.append({"emoji": emoji, "role_id": role_id})
            channel_id = sanitize_id(data.get("channel_id", ""))
            message_id = sanitize_id(data.get("message_id", ""))
            message_template = sanitize_text(
                data.get("message_template", "React below to get your roles."),
                1800,
            )
            show_role_list = boolv(data, "show_role_list", True)

            if boolv(data, "create_message", False) and channel_id:
                async def _create_rr_message():
                    guild_obj = RUNTIME.get_guild(int(guild_id))
                    if guild_obj is None:
                        return ""
                    channel_obj = guild_obj.get_channel(int(channel_id))
                    if channel_obj is None:
                        return ""
                    role_lines = []
                    for pair in pairs[:40]:
                        em = str(pair.get("emoji", "")).strip()
                        rid = str(pair.get("role_id", "")).strip()
                        if not em or not rid:
                            continue
                        role_lines.append(f"{em} - <@&{rid}>")
                    roles_block = "\n".join(role_lines)
                    content = message_template or "React below to get your roles."
                    if "{roles}" in content:
                        content = content.replace("{roles}", roles_block or "No roles configured.")
                    elif show_role_list and roles_block:
                        content = f"{content}\n\n{roles_block}"
                    if len(content) > 1990:
                        content = content[:1990]
                    msg = await channel_obj.send(content)
                    for pair in pairs[:40]:
                        try:
                            await msg.add_reaction(str(pair.get("emoji", "")))
                        except Exception:
                            LOGGER.warning(
                                "reaction_roles_add_reaction_failed guild_id=%s channel_id=%s message_id=%s emoji=%s",
                                guild_id,
                                channel_id,
                                msg.id,
                                pair.get("emoji", ""),
                                exc_info=True,
                            )
                    return str(msg.id)

                try:
                    created_message_id = run_on_bot_loop(_create_rr_message(), timeout_sec=20) or ""
                    if created_message_id:
                        message_id = created_message_id
                except Exception:
                    LOGGER.warning("reaction_roles_create_message_failed guild_id=%s", guild_id, exc_info=True)

            payload = {
                "enabled": boolv(data, "enabled", False),
                "channel_id": channel_id,
                "message_id": message_id,
                "message_template": message_template,
                "show_role_list": show_role_list,
                "pairs": pairs[:40],
            }
        elif module_name == "leveling_system":
            payload = {
                "enabled": boolv(data, "enabled", False),
                "xp_per_message": intv(data, "xp_per_message", 10, 1, 200),
                "cooldown_seconds": intv(data, "cooldown_seconds", 30, 5, 600),
                "level_up_channel_id": sanitize_id(data.get("level_up_channel_id", "")),
            }
        elif module_name == "webhooks":
            payload = {
                "enabled": boolv(data, "enabled", False),
                "notify_url": sanitize_text(data.get("notify_url", ""), 500),
                "events": [sanitize_text(x, 64) for x in listv(data, "events") if sanitize_text(x, 64)][:30],
            }
        elif module_name == "integrations":
            payload = {
                "enabled": boolv(data, "enabled", False),
                "api_base_url": sanitize_text(data.get("api_base_url", ""), 300),
                "api_key_hint": sanitize_text(data.get("api_key_hint", ""), 80),
                "sync_interval_minutes": intv(data, "sync_interval_minutes", 15, 1, 1440),
            }
        elif module_name == "premium":
            tier = sanitize_text(data.get("tier", "free"), 16).lower()
            if tier not in {"free", "pro", "enterprise"}:
                tier = "free"
            payload = {
                "enabled": boolv(data, "enabled", False),
                "tier": tier,
                "limits_override": boolv(data, "limits_override", False),
            }
        elif module_name == "settings":
            current_settings = ((CONFIG_SERVICE.get_guild_config(str(guild_id)).get("modules", {}) or {}).get("settings", {}) or {})
            verify_enabled = boolv(data, "verify_enabled", True)
            lock_enabled = boolv(data, "lock_enabled", False)
            verified_role_id = sanitize_id(data.get("verified_role_id", "")) or sanitize_id(data.get("verify_role_id", ""))
            verify_channel_id = sanitize_id(data.get("verify_channel_id", "")) or sanitize_id(data.get("verify_panel_channel_id", ""))
            lock_mode = sanitize_text(data.get("lock_mode", "all_except_verify"), 32)
            if lock_mode not in {"all_except_verify", "selected_categories"}:
                lock_mode = "all_except_verify"
            strict_hide_unverified = boolv(data, "strict_hide_unverified", False)
            verify_lock_category_ids = [sanitize_id(x) for x in listv(data, "verify_lock_category_ids") if sanitize_id(x)][:100]
            ticket_panel_channel_id = sanitize_id(data.get("ticket_panel_channel_id", ""))
            ticket_panel_title = sanitize_text(data.get("ticket_panel_title", "Need Support?"), 120)
            ticket_panel_body = sanitize_text(
                data.get("ticket_panel_body", "New here or need help? Click the button below to open a private support ticket."),
                1200,
            )
            ticket_panel_message_id = sanitize_id(data.get("ticket_panel_message_id", ""))
            verify_role_id = sanitize_id(data.get("verify_role_id", ""))
            verify_min_account_age_days = intv(data, "verify_min_account_age_days", 7, 0, 365)
            verify_panel_channel_id = sanitize_id(data.get("verify_panel_channel_id", ""))
            verify_panel_title = sanitize_text(data.get("verify_panel_title", "Server Verification"), 120)
            verify_panel_body = sanitize_text(
                data.get("verify_panel_body", "Click the button below to verify and unlock full access."),
                1200,
            )
            verify_panel_message_id = sanitize_id(data.get("verify_panel_message_id", ""))
            verify_visible_channel_ids = [sanitize_id(x) for x in listv(data, "verify_visible_channel_ids") if sanitize_id(x)][:200]
            payload = {
                "enabled": boolv(data, "enabled", True),
                "language": sanitize_text(data.get("language", "en"), 12),
                "timezone": sanitize_text(data.get("timezone", "UTC"), 64),
                "alerts_enabled": boolv(data, "alerts_enabled", True),
                "verify_enabled": verify_enabled,
                "lock_enabled": lock_enabled,
                "verified_role_id": verified_role_id,
                "verify_channel_id": verify_channel_id,
                "lock_mode": lock_mode,
                "strict_hide_unverified": strict_hide_unverified,
                "verify_lock_category_ids": verify_lock_category_ids,
                "verify_lock_last_applied_channel_ids": listv(data, "verify_lock_last_applied_channel_ids")
                or current_settings.get("verify_lock_last_applied_channel_ids", []),
                "verify_lock_last_applied_role_ids": listv(data, "verify_lock_last_applied_role_ids")
                or current_settings.get("verify_lock_last_applied_role_ids", []),
                "ticket_panel_channel_id": ticket_panel_channel_id,
                "ticket_panel_title": ticket_panel_title,
                "ticket_panel_body": ticket_panel_body,
                "ticket_panel_message_id": ticket_panel_message_id,
                "verify_role_id": verified_role_id or verify_role_id,
                "verify_min_account_age_days": verify_min_account_age_days,
                "verify_panel_channel_id": verify_panel_channel_id or verify_channel_id,
                "verify_panel_title": verify_panel_title,
                "verify_panel_body": verify_panel_body,
                "verify_panel_message_id": verify_panel_message_id,
                "verify_visible_channel_ids": verify_visible_channel_ids,
            }
            if boolv(data, "create_ticket_panel", False) and ticket_panel_channel_id:
                async def _create_ticket_panel():
                    guild_obj = RUNTIME.get_guild(int(guild_id))
                    if guild_obj is None:
                        return ""
                    channel_obj = guild_obj.get_channel(int(ticket_panel_channel_id))
                    if channel_obj is None:
                        return ""
                    title = ticket_panel_title or "Need Support?"
                    body = ticket_panel_body or "New here or need help? Click the button below to open a private support ticket."
                    embed = discord.Embed(
                        title=title,
                        description=f"{body}\n\nA staff member will assist you shortly.",
                        color=discord.Colour.blurple(),
                    )
                    view = discord.ui.View(timeout=None)
                    view.add_item(
                        discord.ui.Button(
                            label="Open Ticket",
                            style=discord.ButtonStyle.primary,
                            custom_id="ticket:open",
                        )
                    )
                    msg = await channel_obj.send(embed=embed, view=view)
                    return str(msg.id)

                try:
                    created_message_id = run_on_bot_loop(_create_ticket_panel(), timeout_sec=20) or ""
                    if created_message_id:
                        payload["ticket_panel_message_id"] = created_message_id
                except Exception:
                    LOGGER.warning("ticket_panel_create_failed guild_id=%s", guild_id, exc_info=True)
            if boolv(data, "create_verify_panel", False) and verify_panel_channel_id:
                async def _create_verify_panel():
                    guild_obj = RUNTIME.get_guild(int(guild_id))
                    if guild_obj is None:
                        return ""
                    channel_obj = guild_obj.get_channel(int(verify_panel_channel_id))
                    if channel_obj is None:
                        return ""
                    title = verify_panel_title or "Server Verification"
                    body = verify_panel_body or "Click the button below to verify and unlock full access."
                    verify_url = f"{VERIFY_BASE_URL}/verify/{guild_id}"
                    embed = discord.Embed(
                        title=title,
                        description=f"{body}\n\nThis helps protect the server against raids.",
                        color=discord.Colour.green(),
                    )
                    view = discord.ui.View(timeout=None)
                    view.add_item(
                        discord.ui.Button(
                            label="Verify",
                            style=discord.ButtonStyle.link,
                            url=verify_url,
                        )
                    )
                    msg = await channel_obj.send(embed=embed, view=view)
                    return str(msg.id)

                try:
                    created_verify_message_id = run_on_bot_loop(_create_verify_panel(), timeout_sec=20) or ""
                    if created_verify_message_id:
                        payload["verify_panel_message_id"] = created_verify_message_id
                except Exception:
                    LOGGER.warning("verify_panel_create_failed guild_id=%s", guild_id, exc_info=True)
            if boolv(data, "apply_verify_visibility", False):
                try:
                    ok, detail = run_on_bot_loop(_apply_verify_channel_visibility(str(guild_id)), timeout_sec=45)
                    if not ok:
                        LOGGER.warning("verify_visibility_apply_failed guild_id=%s detail=%s", guild_id, detail)
                except Exception:
                    LOGGER.warning("verify_visibility_apply_exception guild_id=%s", guild_id, exc_info=True)
        else:
            return jsonify({"ok": False, "error": "coming_soon"}), 400

        CONFIG_SERVICE.update_guild_config(str(guild_id), module_name, payload)
        REALTIME_SERVICE.publish(str(guild_id), {"type": "module_saved", "module": module_name, "ts": int(time.time())})
        uid, uname = current_user_meta()
        _audit_log(str(guild_id), "dashboard_module_saved", uid, uname, metadata={"module": str(module_name)})
        run_async(METRICS.track(str(guild_id), f"module_save:{module_name}", {"module": module_name}))
        return jsonify({"ok": True})

    return app


APP = create_app()
validate_critical_dependencies()


def run_dashboard():
    serve(APP, host="127.0.0.1", port=5000, threads=12)
