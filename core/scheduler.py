import asyncio
import ast
import heapq
import json
import logging
import os
import time
import uuid

from repositories.scheduler_repository import SchedulerRepository


class TaskScheduler:
    def __init__(self, repo):
        self.repo = repo
        self.scheduler_repository = SchedulerRepository(repo.pool)
        self._heap = []
        self._known_ids = set()
        self._running = False
        self._bootstrapped = False
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger("discordbot.scheduler")
        self._handler_timeout_sec = float(os.getenv("TASK_HANDLER_TIMEOUT_SEC", "15"))
        self._refresh_interval_sec = float(os.getenv("SCHEDULER_REFRESH_SEC", "30"))
        self._last_refresh_at = 0.0

    @staticmethod
    def _parse_payload(raw: str):
        try:
            return json.loads(raw)
        except Exception:
            try:
                parsed = ast.literal_eval(raw)
                return parsed if isinstance(parsed, dict) else {}
            except Exception:
                return {}

    async def add_task(self, guild_id: str, run_at: int, priority: int, payload: dict):
        task_id = str(uuid.uuid4())
        async with self._lock:
            heapq.heappush(self._heap, (run_at, priority, task_id, guild_id, payload, 0))
            self._known_ids.add(task_id)
        await self.scheduler_repository.insert_task(task_id, guild_id, run_at, priority, payload)
        return task_id

    async def load_pending(self, reset: bool = False):
        await self.scheduler_repository.reset_running_to_pending()
        rows = await self.scheduler_repository.load_pending()
        if reset:
            async with self._lock:
                self._heap.clear()
                self._known_ids.clear()
        loaded = 0
        for row in rows:
            task_id = str(row.get("id", ""))
            payload = self._parse_payload(row.get("payload", "{}"))
            if not task_id:
                continue
            async with self._lock:
                if task_id in self._known_ids:
                    continue
                heapq.heappush(
                    self._heap,
                    (
                        int(row.get("run_at", int(time.time()))),
                        int(row.get("priority", 0)),
                        task_id,
                        str(row.get("guild_id", "")),
                        payload,
                        0,
                    ),
                )
                self._known_ids.add(task_id)
                loaded += 1
        if reset or loaded:
            self._logger.info("scheduler_loaded_pending loaded=%s queued=%s", loaded, len(self._heap))

    async def run(self, handler):
        self._running = True
        if not self._bootstrapped:
            try:
                await self.load_pending(reset=True)
            except Exception:
                self._logger.exception("scheduler_bootstrap_load_failed")
            self._bootstrapped = True
        while self._running:
            nowf = time.time()
            if (nowf - self._last_refresh_at) >= self._refresh_interval_sec:
                try:
                    await self.load_pending(reset=False)
                    self._last_refresh_at = nowf
                except Exception:
                    self._logger.exception("scheduler_periodic_refresh_failed")
            now = int(time.time())
            async with self._lock:
                has_items = bool(self._heap)
            if not has_items:
                await asyncio.sleep(0.4)
                continue
            async with self._lock:
                run_at, priority, task_id, guild_id, payload, retries = self._heap[0]
            if run_at > now:
                await asyncio.sleep(min(1.0, run_at - now))
                continue
            async with self._lock:
                heapq.heappop(self._heap)
                self._known_ids.discard(task_id)
            try:
                claimed = await self.scheduler_repository.claim_task(task_id)
                if claimed == 0:
                    continue
                await asyncio.wait_for(handler(guild_id, payload), timeout=self._handler_timeout_sec)
                await self.scheduler_repository.set_status(task_id, "done")
            except asyncio.TimeoutError:
                self._logger.warning(
                    "scheduler_task_timeout task_id=%s guild_id=%s timeout_sec=%s retries=%s",
                    task_id,
                    guild_id,
                    self._handler_timeout_sec,
                    retries,
                )
                if retries < 3:
                    next_run = now + (2 ** retries)
                    async with self._lock:
                        heapq.heappush(self._heap, (next_run, priority, task_id, guild_id, payload, retries + 1))
                        self._known_ids.add(task_id)
                    await self.scheduler_repository.set_status(task_id, "pending", run_at=next_run)
                    self._logger.info(
                        "scheduler_task_retry_scheduled task_id=%s guild_id=%s next_run=%s retry=%s",
                        task_id,
                        guild_id,
                        next_run,
                        retries + 1,
                    )
                else:
                    await self.scheduler_repository.set_status(task_id, "dead")
                    self._logger.error(
                        "scheduler_task_dead task_id=%s guild_id=%s",
                        task_id,
                        guild_id,
                    )
            except Exception:
                self._logger.warning(
                    "scheduler_task_failed task_id=%s guild_id=%s retries=%s",
                    task_id,
                    guild_id,
                    retries,
                    exc_info=True,
                )
                if retries < 3:
                    next_run = now + (2 ** retries)
                    async with self._lock:
                        heapq.heappush(self._heap, (next_run, priority, task_id, guild_id, payload, retries + 1))
                        self._known_ids.add(task_id)
                    await self.scheduler_repository.set_status(task_id, "pending", run_at=next_run)
                    self._logger.info(
                        "scheduler_task_retry_scheduled task_id=%s guild_id=%s next_run=%s retry=%s",
                        task_id,
                        guild_id,
                        next_run,
                        retries + 1,
                    )
                else:
                    await self.scheduler_repository.set_status(task_id, "dead")
                    self._logger.error(
                        "scheduler_task_dead task_id=%s guild_id=%s",
                        task_id,
                        guild_id,
                    )
            await self.scheduler_repository.cleanup_done_before(now - 86400)

    def stop(self):
        self._running = False

    def queue_length(self):
        return len(self._heap)
