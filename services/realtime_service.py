"""Realtime service wrapper around bus and websocket auth token issuance."""

from __future__ import annotations

import time


class RealtimeService:
    """Publish realtime payloads and issue websocket auth tokens."""

    def __init__(self, bus, ws_auth):
        self.bus = bus
        self.ws_auth = ws_auth

    def publish(self, guild_id: str, payload: dict) -> None:
        self.bus.publish(str(guild_id), dict(payload))

    def publish_support_global(self, payload: dict) -> None:
        self.bus.publish("support:global", dict(payload))

    def support_case_event(self, guild_id: str, case_id: int, event: str) -> dict:
        payload = {
            "type": "support_case_update",
            "event": str(event),
            "case_id": int(case_id),
            "guild_id": str(guild_id),
            "ts": int(time.time()),
        }
        self.publish(str(guild_id), payload)
        self.publish_support_global(payload)
        return payload

    def issue_ws_token(self, user_id: str, guild_id: str, ttl_sec: int = 120) -> str:
        return self.ws_auth.issue(str(user_id), str(guild_id), ttl_sec=ttl_sec)
