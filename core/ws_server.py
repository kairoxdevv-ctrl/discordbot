import asyncio
import json
import logging
import threading
import time
from queue import Empty
from urllib.parse import parse_qs, urlparse

import websockets


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


async def _client_handler(websocket, bus, auth_manager):
    parsed = urlparse(websocket.request.path)
    qs = parse_qs(parsed.query)
    token = (qs.get("token") or [""])[0]
    guild_id = (qs.get("guild") or [""])[0]

    if not token or not guild_id or not auth_manager.validate(token, guild_id):
        await websocket.close(code=4001, reason="unauthorized")
        return

    q = bus.subscribe(str(guild_id))
    try:
        while True:
            try:
                payload = await asyncio.to_thread(q.get, True, 20)
            except Empty:
                payload = json.dumps({"type": "ping", "t": int(time.time())})
            try:
                await websocket.send(payload)
            except websockets.exceptions.ConnectionClosed:
                break
    except asyncio.CancelledError:
        raise
    except Exception:
        LOGGER.warning("ws_client_loop_failed guild_id=%s", guild_id, exc_info=True)
    finally:
        bus.unsubscribe(str(guild_id), q)
        try:
            await websocket.close()
        except Exception:
            LOGGER.warning("ws_close_failed guild_id=%s", guild_id, exc_info=True)


async def _server_main(host, port, bus, auth_manager):
    async with websockets.serve(lambda ws: _client_handler(ws, bus, auth_manager), host, port, ping_interval=20, ping_timeout=20):
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
