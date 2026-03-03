import asyncio
import json
import logging
import os
import threading
import time
from queue import Empty
from urllib.parse import parse_qs, urlparse

import websockets
from websockets.server import WebSocketServerProtocol


class WsAuthManager:
    def __init__(self):
        self._tokens = {}
        self._lock = threading.RLock()

    def _prune_expired(self, now: int):
        expired = [tok for tok, item in self._tokens.items() if int(item.get("exp", 0)) < now]
        for tok in expired:
            self._tokens.pop(tok, None)

    def issue(self, user_id: str, guild_id: str, ttl_sec: int = 120):
        import secrets

        token = secrets.token_urlsafe(32)
        now = int(time.time())
        ttl = max(30, min(600, int(ttl_sec or 120)))
        with self._lock:
            self._prune_expired(now)
            self._tokens[token] = {
                "user_id": str(user_id),
                "guild_id": str(guild_id),
                "exp": now + ttl,
            }
        return token

    def validate(self, token: str, guild_id: str):
        now = int(time.time())
        with self._lock:
            self._prune_expired(now)
            item = self._tokens.get(token)
            if not item:
                return False
            if str(item["guild_id"]) != str(guild_id):
                return False
            # Single-use token: consume on successful validation.
            self._tokens.pop(token, None)
            return True


LOGGER = logging.getLogger("discordbot.ws")
MAX_WS_PAYLOAD_BYTES = max(1024, min(2 * 1024 * 1024, int(os.getenv("WS_MAX_PAYLOAD_BYTES", "262144"))))
WS_IDLE_TIMEOUT_SEC = max(20, min(600, int(os.getenv("WS_IDLE_TIMEOUT_SEC", "120"))))
WS_MAX_CONNECTIONS = max(10, min(10000, int(os.getenv("WS_MAX_CONNECTIONS", "2000"))))
WS_MAX_CONN_PER_GUILD = max(1, min(500, int(os.getenv("WS_MAX_CONN_PER_GUILD", "200"))))


class WsConnectionRegistry:
    def __init__(self):
        self._lock = threading.RLock()
        self._total = 0
        self._guild_counts: dict[str, int] = {}

    def try_open(self, guild_id: str) -> bool:
        with self._lock:
            if self._total >= WS_MAX_CONNECTIONS:
                return False
            per_guild = int(self._guild_counts.get(guild_id, 0))
            if per_guild >= WS_MAX_CONN_PER_GUILD:
                return False
            self._total += 1
            self._guild_counts[guild_id] = per_guild + 1
            return True

    def close(self, guild_id: str):
        with self._lock:
            self._total = max(0, self._total - 1)
            per_guild = int(self._guild_counts.get(guild_id, 0))
            if per_guild <= 1:
                self._guild_counts.pop(guild_id, None)
            else:
                self._guild_counts[guild_id] = per_guild - 1


WS_CONNECTIONS = WsConnectionRegistry()


async def _client_handler(websocket: WebSocketServerProtocol, bus, auth_manager):
    parsed = urlparse(websocket.request.path)
    qs = parse_qs(parsed.query)
    token = (qs.get("token") or [""])[0]
    guild_id = (qs.get("guild") or [""])[0]

    if not token or not guild_id or not auth_manager.validate(token, guild_id):
        await websocket.close(code=4001, reason="unauthorized")
        return
    if not WS_CONNECTIONS.try_open(str(guild_id)):
        await websocket.close(code=4003, reason="capacity")
        return

    q = bus.subscribe(str(guild_id))
    last_sent_at = time.time()
    try:
        while True:
            try:
                payload = await asyncio.to_thread(q.get, True, 20)
            except Empty:
                payload = json.dumps({"type": "ping", "t": int(time.time())})
            if (time.time() - last_sent_at) > WS_IDLE_TIMEOUT_SEC:
                await websocket.close(code=4000, reason="idle_timeout")
                break
            if len(payload.encode("utf-8")) > MAX_WS_PAYLOAD_BYTES:
                LOGGER.warning("ws_payload_too_large guild_id=%s bytes=%s", guild_id, len(payload.encode("utf-8")))
                continue
            try:
                await websocket.send(payload)
                last_sent_at = time.time()
            except websockets.exceptions.ConnectionClosed:
                break
    except asyncio.CancelledError:
        raise
    except Exception:
        LOGGER.warning("ws_client_loop_failed guild_id=%s", guild_id, exc_info=True)
    finally:
        bus.unsubscribe(str(guild_id), q)
        WS_CONNECTIONS.close(str(guild_id))
        try:
            await websocket.close()
        except Exception:
            LOGGER.warning("ws_close_failed guild_id=%s", guild_id, exc_info=True)


async def _server_main(host, port, bus, auth_manager):
    async with websockets.serve(
        lambda ws: _client_handler(ws, bus, auth_manager),
        host,
        port,
        ping_interval=20,
        ping_timeout=20,
        max_size=MAX_WS_PAYLOAD_BYTES,
        max_queue=64,
    ):
        await asyncio.Future()


def start_ws_server(host, port, bus, auth_manager):
    def _run():
        try:
            asyncio.run(_server_main(host, port, bus, auth_manager))
        except Exception:
            LOGGER.exception("ws_server_start_failed host=%s port=%s", host, port)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t
