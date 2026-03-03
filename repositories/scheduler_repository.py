"""Repository for scheduled task persistence."""

from __future__ import annotations

import json


class SchedulerRepository:
    """Read and write scheduled tasks from storage."""

    def __init__(self, pool):
        self.pool = pool

    async def insert_task(self, task_id: str, guild_id: str, run_at: int, priority: int, payload: dict) -> None:
        """Persist or replace scheduled task as pending."""
        await self.pool.execute(
            "INSERT OR REPLACE INTO scheduled_tasks (id, guild_id, run_at, priority, payload, status) VALUES (?, ?, ?, ?, ?, ?)",
            (str(task_id), str(guild_id), int(run_at), int(priority), json.dumps(payload, ensure_ascii=False), "pending"),
        )

    async def claim_task(self, task_id: str) -> int:
        """Atomically claim a pending task and mark it running."""
        return await self.pool.execute(
            "UPDATE scheduled_tasks SET status='running' WHERE id=? AND status='pending'",
            (str(task_id),),
        )

    async def set_status(self, task_id: str, status: str, run_at: int | None = None) -> None:
        """Update task status, optionally overriding next run timestamp."""
        if run_at is None:
            await self.pool.execute("UPDATE scheduled_tasks SET status=? WHERE id=?", (str(status), str(task_id)))
            return
        await self.pool.execute(
            "UPDATE scheduled_tasks SET status=?, run_at=? WHERE id=?",
            (str(status), int(run_at), str(task_id)),
        )

    async def load_pending(self) -> list[dict]:
        """Load pending tasks sorted by run time and priority."""
        return await self.pool.fetchall(
            """
            SELECT id, guild_id, run_at, priority, payload
            FROM scheduled_tasks
            WHERE status='pending'
            ORDER BY run_at ASC, priority ASC
            """
        )

    async def reset_running_to_pending(self) -> None:
        """Recover in-flight tasks after restart by resetting to pending."""
        await self.pool.execute("UPDATE scheduled_tasks SET status='pending' WHERE status='running'")

    async def cleanup_done_before(self, ts: int) -> None:
        """Delete completed tasks older than retention cutoff timestamp."""
        await self.pool.execute("DELETE FROM scheduled_tasks WHERE status='done' AND run_at < ?", (int(ts),))
