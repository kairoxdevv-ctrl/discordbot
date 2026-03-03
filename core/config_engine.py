"""Core layer: persistent configuration engine with schema normalization and caching."""

import json
import logging
import os
import sqlite3
import threading
import time
from collections import OrderedDict
from pathlib import Path

from jsonschema import ValidationError, validate

CONFIG_SCHEMA_V3 = {
    "type": "object",
    "patternProperties": {
        "^[0-9]+$": {
            "type": "object",
            "properties": {
                "schema_version": {"type": "integer"},
                "modules": {
                    "type": "object",
                    "properties": {
                        "welcome": {"type": "object"},
                        "autorole": {"type": "object"},
                        "automod": {"type": "object"},
                        "logging": {"type": "object"},
                    },
                    "required": ["welcome", "autorole", "automod", "logging"],
                },
            },
            "required": ["modules"],
        }
    },
}
LOGGER = logging.getLogger("discordbot.config_engine")


class RWLock:
    """Simple reentrant lock wrapper used by config engine critical sections."""

    def __init__(self):
        self._lock = threading.RLock()

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._lock.release()


class ConfigEngineV3:
    """SQLite-backed config engine.

    Keeps the public API unchanged while avoiding unbounded config.bak file growth.
    """

    def __init__(self, path: Path, defaults_factory, cache_size: int = 256, on_reload=None, db_path: Path | None = None):
        self.path = path
        self.db_path = db_path or (self.path.parent / "platform.db")
        self.defaults_factory = defaults_factory
        self.on_reload = on_reload
        self.cache_size = max(32, cache_size)
        self.lock = RWLock()
        self._cache = OrderedDict()
        self._data = {}
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._ensure_schema()
        self._payload_col = self._detect_payload_column()
        self._ensure_file()
        self._cleanup_legacy_backup_files()
        self._migrate_file_if_needed()
        self._load(force=True)

    def close(self):
        """Close underlying SQLite connection for graceful shutdown."""
        try:
            self._conn.close()
        except Exception:
            LOGGER.warning("config_db_close_failed", exc_info=True)

    def _ensure_schema(self):
        cur = self._conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS guild_configs (
              guild_id TEXT PRIMARY KEY,
              payload TEXT NOT NULL,
              updated_at INTEGER NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_guild_configs_updated ON guild_configs (updated_at DESC)")
        self._conn.commit()

    def _detect_payload_column(self) -> str:
        cur = self._conn.cursor()
        cur.execute("PRAGMA table_info(guild_configs)")
        cols = {str(r["name"]) for r in cur.fetchall()}
        if "payload" in cols:
            return "payload"
        if "data" in cols:
            return "data"
        cur.execute("ALTER TABLE guild_configs ADD COLUMN payload TEXT NOT NULL DEFAULT '{}'")
        self._conn.commit()
        return "payload"

    def _payload_column_sql(self) -> str:
        if self._payload_col == "payload":
            return "payload"
        if self._payload_col == "data":
            return "data"
        raise RuntimeError("invalid_payload_column")

    def _ensure_file(self):
        # Keep config.json present for compatibility/debugging, but no .bak rotation.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text("{}", encoding="utf-8")

    def _cleanup_legacy_backup_files(self):
        # Old versions produced many config.bak.<timestamp> files; retain only a small tail.
        keep = max(0, int(os.getenv("CONFIG_BAK_KEEP", "10") or "10"))
        max_age_days = max(0, int(os.getenv("CONFIG_BAK_MAX_AGE_DAYS", "14") or "14"))
        now = int(time.time())
        cutoff = now - (max_age_days * 86400)
        candidates = sorted(
            self.path.parent.glob(f"{self.path.stem}.bak.*"),
            key=lambda p: p.stat().st_mtime if p.exists() else 0,
            reverse=True,
        )
        removed = 0
        for idx, bak_path in enumerate(candidates):
            should_prune = idx >= keep
            try:
                if not should_prune and max_age_days > 0:
                    should_prune = int(bak_path.stat().st_mtime) < cutoff
                if should_prune:
                    bak_path.unlink(missing_ok=True)
                    removed += 1
            except Exception:
                LOGGER.warning("config_backup_cleanup_failed path=%s", bak_path, exc_info=True)
        if removed:
            LOGGER.info("config_backup_cleanup_removed count=%s", removed)

    def _normalize_entry(self, entry):
        defaults = self.defaults_factory()
        changed = False

        if not isinstance(entry, dict):
            return defaults, True

        modules = entry.get("modules")
        if not isinstance(modules, dict):
            modules = {}
            changed = True

        legacy_keys = ("channel_id", "title", "description", "color", "enabled", "image", "thumbnail", "footer")
        legacy_welcome = {}
        for key in legacy_keys:
            if key in entry:
                legacy_welcome[key] = entry.get(key)

        if legacy_welcome:
            cur_welcome = modules.get("welcome", {})
            if not isinstance(cur_welcome, dict):
                cur_welcome = {}
            merged = dict(legacy_welcome)
            merged.update(cur_welcome)
            modules["welcome"] = merged
            changed = True

        normalized = {"schema_version": 3, "modules": {}}
        for mod_name, mod_defaults in defaults["modules"].items():
            payload = modules.get(mod_name, {})
            if not isinstance(payload, dict):
                payload = {}
                changed = True
            merged = dict(mod_defaults)
            merged.update(payload)
            normalized["modules"][mod_name] = merged

        for k, v in entry.items():
            if k not in ("modules", "schema_version") and k not in legacy_keys:
                normalized[k] = v
        return normalized, changed

    def _validate(self, data):
        validate(instance=data, schema=CONFIG_SCHEMA_V3)

    def _read_file_data(self):
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, dict) else {}
        except Exception:
            LOGGER.warning("config_file_read_failed path=%s", self.path, exc_info=True)
            return {}

    def _count_rows(self) -> int:
        cur = self._conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM guild_configs")
        row = cur.fetchone()
        return int((row or {}).get("c", 0) if isinstance(row, dict) else row["c"])

    def _migrate_file_if_needed(self):
        if self._count_rows() > 0:
            return

        file_data = self._read_file_data()
        if not file_data:
            return

        upgraded = {}
        for gid, entry in file_data.items():
            norm, _ = self._normalize_entry(entry)
            upgraded[str(gid)] = norm

        try:
            self._validate(upgraded)
        except ValidationError:
            LOGGER.error("config_migration_validation_failed", exc_info=True)
            upgraded = {}

        if not upgraded:
            return

        now = int(time.time())
        cur = self._conn.cursor()
        col = self._payload_column_sql()
        for gid, payload in upgraded.items():
            cur.execute(
                f"INSERT OR REPLACE INTO guild_configs (guild_id, {col}, updated_at) VALUES (?, ?, ?)",
                (str(gid), json.dumps(payload, ensure_ascii=False), now),
            )
        self._conn.commit()
        LOGGER.info("config_migrated_to_sqlite guilds=%s", len(upgraded))

    def _load(self, force=False):
        with self.lock:
            cur = self._conn.cursor()
            col = self._payload_column_sql()
            cur.execute(f"SELECT guild_id, {col} AS payload FROM guild_configs")
            rows = cur.fetchall()
            upgraded = {}
            migrated = False
            for row in rows:
                gid = str(row["guild_id"])
                try:
                    raw_payload = json.loads(row["payload"])
                except Exception:
                    raw_payload = {}
                    migrated = True
                norm, changed = self._normalize_entry(raw_payload)
                upgraded[gid] = norm
                migrated = migrated or changed

            try:
                self._validate(upgraded)
            except ValidationError:
                LOGGER.error("config_schema_validation_failed", exc_info=True)
                if self._data:
                    return
                upgraded = {}

            self._data = upgraded
            self._cache.clear()

            if migrated and upgraded:
                self._persist_all_locked()

    def _persist_all_locked(self):
        now = int(time.time())
        cur = self._conn.cursor()
        cur.execute("DELETE FROM guild_configs")
        col = self._payload_column_sql()
        for gid, payload in self._data.items():
            cur.execute(
                f"INSERT OR REPLACE INTO guild_configs (guild_id, {col}, updated_at) VALUES (?, ?, ?)",
                (str(gid), json.dumps(payload, ensure_ascii=False), now),
            )
        self._conn.commit()

    def _persist_guild_locked(self, gid: str, payload: dict):
        cur = self._conn.cursor()
        col = self._payload_column_sql()
        cur.execute(
            f"INSERT OR REPLACE INTO guild_configs (guild_id, {col}, updated_at) VALUES (?, ?, ?)",
            (str(gid), json.dumps(payload, ensure_ascii=False), int(time.time())),
        )
        self._conn.commit()

    def get_all(self):
        """Return deep-copied map of all guild configuration payloads."""
        self._load()
        with self.lock:
            return json.loads(json.dumps(self._data))

    def get_guild(self, guild_id):
        """Return deep-copied guild config, creating defaults when absent."""
        self._load()
        gid = str(guild_id)
        with self.lock:
            if gid in self._cache:
                entry = self._cache.pop(gid)
                self._cache[gid] = entry
                return json.loads(json.dumps(entry))

            entry = self._data.get(gid)
            if entry is None:
                entry = self._normalize_entry({})[0]
                self._data[gid] = entry
                self._persist_guild_locked(gid, entry)

            self._cache[gid] = entry
            while len(self._cache) > self.cache_size:
                self._cache.popitem(last=False)
            return json.loads(json.dumps(entry))

    def save_module(self, guild_id, module_name, payload):
        """Persist one module payload for a guild and trigger reload callback."""
        self._load()
        gid = str(guild_id)
        with self.lock:
            guild_entry = self.get_guild(gid)
            guild_entry.setdefault("modules", {})[module_name] = payload
            guild_entry["schema_version"] = 3
            self._data[gid] = guild_entry
            self._cache[gid] = guild_entry
            while len(self._cache) > self.cache_size:
                self._cache.popitem(last=False)
            self._persist_guild_locked(gid, guild_entry)

        if callable(self.on_reload):
            try:
                self.on_reload()
            except Exception:
                LOGGER.warning("config_on_reload_callback_failed", exc_info=True)
