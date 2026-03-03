import asyncio
import json
import queue
import sqlite3
import threading
import time
from pathlib import Path


class SQLitePool:
    def __init__(self, db_path: Path, size: int = 4):
        self.db_path = str(db_path)
        self.size = max(2, size)
        self._pool = queue.Queue(maxsize=self.size)
        self._init_lock = threading.Lock()
        self._initialized = False
        for _ in range(self.size):
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA journal_mode=WAL")
            self._pool.put(conn)

    def init_schema(self):
        with self._init_lock:
            if self._initialized:
                return
            conn = self.acquire()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      guild_id TEXT NOT NULL,
                      event_type TEXT NOT NULL,
                      created_at INTEGER NOT NULL,
                      payload TEXT NOT NULL DEFAULT '{}'
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_guild_time ON events (guild_id, created_at DESC)")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scheduled_tasks (
                      id TEXT PRIMARY KEY,
                      guild_id TEXT NOT NULL,
                      run_at INTEGER NOT NULL,
                      priority INTEGER NOT NULL,
                      payload TEXT NOT NULL,
                      status TEXT NOT NULL
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS moderation_warnings (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      guild_id TEXT NOT NULL,
                      user_id TEXT NOT NULL,
                      moderator_id TEXT NOT NULL,
                      reason TEXT NOT NULL DEFAULT '',
                      created_at INTEGER NOT NULL,
                      active INTEGER NOT NULL DEFAULT 1
                    )
                    """
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_warnings_guild_user_active ON moderation_warnings (guild_id, user_id, active)"
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS audit_events (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      guild_id TEXT NOT NULL,
                      actor_id TEXT NOT NULL,
                      actor_name TEXT NOT NULL,
                      action_type TEXT NOT NULL,
                      target_id TEXT NOT NULL DEFAULT '',
                      metadata TEXT NOT NULL DEFAULT '{}',
                      created_at INTEGER NOT NULL
                    )
                    """
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_audit_events_guild_time ON audit_events (guild_id, created_at DESC)"
                )
                conn.commit()
                self._initialized = True
            finally:
                self.release(conn)

    def acquire(self):
        return self._pool.get()

    def release(self, conn):
        self._pool.put(conn)

    async def execute(self, sql, params=()):
        loop = asyncio.get_running_loop()

        def _run():
            conn = self.acquire()
            try:
                cur = conn.cursor()
                cur.execute(sql, params)
                conn.commit()
                return cur.rowcount
            finally:
                self.release(conn)

        return await loop.run_in_executor(None, _run)

    async def fetchone(self, sql, params=()):
        loop = asyncio.get_running_loop()

        def _run():
            conn = self.acquire()
            try:
                cur = conn.cursor()
                cur.execute(sql, params)
                row = cur.fetchone()
                return dict(row) if row else None
            finally:
                self.release(conn)

        return await loop.run_in_executor(None, _run)

    async def fetchall(self, sql, params=()):
        loop = asyncio.get_running_loop()

        def _run():
            conn = self.acquire()
            try:
                cur = conn.cursor()
                cur.execute(sql, params)
                return [dict(r) for r in cur.fetchall()]
            finally:
                self.release(conn)

        return await loop.run_in_executor(None, _run)


class Repository:
    def __init__(self, pool: SQLitePool):
        self.pool = pool

    async def add_event(self, guild_id: str, event_type: str, created_at: int, payload: dict):
        await self.pool.execute(
            "INSERT INTO events (guild_id, event_type, created_at, payload) VALUES (?, ?, ?, ?)",
            (guild_id, event_type, created_at, json.dumps(payload, ensure_ascii=False)),
        )

    async def aggregate_events(self, guild_id: str, from_ts: int):
        rows = await self.pool.fetchall(
            """
            SELECT event_type, COUNT(*) AS c
            FROM events
            WHERE guild_id=? AND created_at>=?
            GROUP BY event_type
            """,
            (guild_id, from_ts),
        )
        return {r["event_type"]: r["c"] for r in rows}

    async def add_audit_event(
        self,
        guild_id: str,
        actor_id: str,
        actor_name: str,
        action_type: str,
        target_id: str = "",
        metadata: dict | None = None,
        created_at: int = 0,
    ):
        await self.pool.execute(
            """
            INSERT INTO audit_events (guild_id, actor_id, actor_name, action_type, target_id, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(guild_id),
                str(actor_id),
                str(actor_name)[:120],
                str(action_type)[:120],
                str(target_id)[:120],
                json.dumps(metadata or {}, ensure_ascii=False),
                int(created_at or time.time()),
            ),
        )
