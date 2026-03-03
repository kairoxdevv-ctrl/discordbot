import json
import logging
import os
import queue
import re
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from urllib import error as urlerror
from urllib import request as urlrequest


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
            "pid": os.getpid(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if hasattr(record, "extra") and isinstance(record.extra, dict):
            payload.update(record.extra)
        return json.dumps(payload, ensure_ascii=False)


WEBHOOK_URL_RE = re.compile(r"^https://discord\.com/api/webhooks/\d+/[A-Za-z0-9._-]+$")


def _sanitize_text(text: str) -> str:
    out = str(text or "")
    for key in (
        "DISCORD_TOKEN",
        "CLIENT_SECRET",
        "SECRET_KEY",
        "DISCORD_LOG_WEBHOOK_INFO",
        "DISCORD_LOG_WEBHOOK_WARNING",
        "DISCORD_LOG_WEBHOOK_ERROR",
        "DISCORD_LOG_WEBHOOK_CRITICAL",
    ):
        val = (os.getenv(key, "") or "").strip()
        if len(val) >= 10:
            out = out.replace(val, f"[REDACTED:{key}]")
    return out


class DiscordWebhookHandler(logging.Handler):
    def __init__(self, routes: dict, timeout_sec: float = 4.0, min_level: int = logging.WARNING):
        super().__init__(level=logging.INFO)
        self.routes = routes
        self.timeout_sec = float(timeout_sec)
        self.min_level = int(min_level)
        self.rate_per_min = max(1, int(os.getenv("DISCORD_LOG_WEBHOOK_RATE_PER_MIN", "30")))
        self._recent = defaultdict(deque)
        self._url_name = {v: k for k, v in routes.items() if v}
        self._fail_count = defaultdict(int)
        self._mute_until = defaultdict(float)
        self._dropped = 0
        self._last_drop_report = 0.0
        self.q = queue.Queue(maxsize=1000)
        self._worker = threading.Thread(target=self._loop, daemon=True)
        self._worker.start()

    def _route_for(self, levelno: int) -> str:
        if levelno >= logging.CRITICAL and self.routes.get("critical"):
            return self.routes["critical"]
        if levelno >= logging.ERROR and self.routes.get("error"):
            return self.routes["error"] or self.routes.get("critical", "")
        if levelno >= logging.WARNING and self.routes.get("warning"):
            return self.routes["warning"] or self.routes.get("error", "") or self.routes.get("critical", "")
        return self.routes.get("info", "") or self.routes.get("warning", "") or self.routes.get("error", "") or self.routes.get("critical", "")

    def emit(self, record: logging.LogRecord):
        try:
            if record.levelno < self.min_level:
                return
            raw_msg = record.getMessage()
            if self._should_suppress(record, raw_msg):
                return
            url = self._route_for(record.levelno)
            if not url:
                return
            now = time.time()
            if now < self._mute_until[url]:
                return
            dq = self._recent[url]
            cutoff = now - 60.0
            while dq and dq[0] < cutoff:
                dq.popleft()
            if len(dq) >= self.rate_per_min:
                return
            dq.append(now)
            msg = self.format(record)
            msg = _sanitize_text(msg)
            if len(msg) > 1800:
                msg = msg[:1800] + "..."
            try:
                self.q.put_nowait((url, msg))
            except queue.Full:
                self._dropped += 1
                now = time.time()
                if (now - self._last_drop_report) > 60:
                    sys.stderr.write(f"discord_webhook_log_drop queue_full dropped={self._dropped}\n")
                    sys.stderr.flush()
                    self._last_drop_report = now
        except Exception as exc:
            try:
                sys.stderr.write(f"discord_webhook_emit_failed error={type(exc).__name__}\n")
                sys.stderr.flush()
            except Exception:
                return

    @staticmethod
    def _should_suppress(record: logging.LogRecord, message: str) -> bool:
        logger_name = str(record.name or "")
        msg = str(message or "")
        if logger_name.startswith("discord.client") and "PyNaCl is not installed" in msg:
            return True
        if logger_name.startswith("discord.http") and "We are being rate limited" in msg:
            return True
        return False

    def _post(self, url: str, content: str):
        body = json.dumps({"content": f"```json\n{content}\n```"}, ensure_ascii=False).encode("utf-8")
        req = urlrequest.Request(
            url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "discordbot/1.0",
            },
            method="POST",
        )
        with urlrequest.urlopen(req, timeout=self.timeout_sec):
            return

    def _loop(self):
        while True:
            item = self.q.get()
            if not item:
                continue
            url, msg = item
            try:
                self._post(url, msg)
                self._fail_count[url] = 0
                self._mute_until[url] = 0.0
            except Exception:
                try:
                    # single retry with short backoff
                    time.sleep(0.6)
                    self._post(url, msg)
                    self._fail_count[url] = 0
                    self._mute_until[url] = 0.0
                except urlerror.HTTPError as http_exc:
                    self._fail_count[url] += 1
                    if int(getattr(http_exc, "code", 0)) in {401, 403, 404}:
                        self._mute_until[url] = time.time() + 3600.0
                    elif self._fail_count[url] >= 5:
                        self._mute_until[url] = time.time() + 300.0
                    route_name = self._url_name.get(url, "unknown")
                    sys.stderr.write(
                        f"discord_webhook_log_send_failed route={route_name} status={getattr(http_exc, 'code', 0)} fail_count={self._fail_count[url]}\n"
                    )
                    sys.stderr.flush()
                except Exception:
                    self._fail_count[url] += 1
                    if self._fail_count[url] >= 5:
                        self._mute_until[url] = time.time() + 300.0
                    route_name = self._url_name.get(url, "unknown")
                    sys.stderr.write(
                        f"discord_webhook_log_send_failed route={route_name} fail_count={self._fail_count[url]}\n"
                    )
                    sys.stderr.flush()


def setup_structured_logging(level: str = "INFO"):
    root = logging.getLogger()
    if getattr(root, "_json_logging_configured", False):
        return

    formatter = JsonFormatter()
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    root.handlers.clear()
    root.addHandler(stream_handler)
    webhook_routes = {
        "info": (os.getenv("DISCORD_LOG_WEBHOOK_INFO", "") or "").strip(),
        "warning": (os.getenv("DISCORD_LOG_WEBHOOK_WARNING", "") or "").strip(),
        "error": (os.getenv("DISCORD_LOG_WEBHOOK_ERROR", "") or "").strip(),
        "critical": (os.getenv("DISCORD_LOG_WEBHOOK_CRITICAL", "") or "").strip(),
    }
    valid_routes = {}
    for key, value in webhook_routes.items():
        valid_routes[key] = value if WEBHOOK_URL_RE.match(value) else ""
    if any(valid_routes.values()):
        min_level_name = (os.getenv("DISCORD_LOG_WEBHOOK_MIN_LEVEL", "WARNING") or "WARNING").strip().upper()
        min_level = getattr(logging, min_level_name, logging.WARNING)
        wh = DiscordWebhookHandler(
            valid_routes,
            timeout_sec=float(os.getenv("DISCORD_LOG_WEBHOOK_TIMEOUT_SEC", "4")),
            min_level=min_level,
        )
        wh.setFormatter(formatter)
        root.addHandler(wh)
    root.setLevel(getattr(logging, str(level).upper(), logging.INFO))
    logging.getLogger("discord.http").setLevel(logging.ERROR)
    logging.getLogger("discord.client").setLevel(logging.ERROR)
    logging.getLogger("waitress").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    root._json_logging_configured = True

    logging.getLogger("discordbot.bootstrap").info(
        "structured_logging_enabled",
        extra={"extra": {"startup_unix": int(time.time())}},
    )
