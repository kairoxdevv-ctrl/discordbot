"""Support domain service for dashboard routes."""

from __future__ import annotations

import logging
import time

LOGGER = logging.getLogger("discordbot.services.support")


class SupportService:
    """Business operations for support cases/messages and SLA handling."""

    def __init__(self, repository, realtime_service, audit_callback, send_webhook_callback):
        self.repository = repository
        self.realtime_service = realtime_service
        self.audit_callback = audit_callback
        self.send_webhook_callback = send_webhook_callback

    def get_case(self, case_id: int) -> dict | None:
        """Fetch support case by identifier with defensive type checking."""
        row = self.repository.get_case(int(case_id))
        if row is None:
            return None
        if not isinstance(row, dict):
            LOGGER.warning("support_get_case_invalid_row case_id=%s", case_id)
            return None
        return row

    def list_cases(self, where_sql: str, params: tuple, query_text: str) -> list[dict]:
        """List support cases and apply optional text filtering."""
        rows = self.repository.query_cases(where_sql, params)
        if not isinstance(rows, list):
            LOGGER.warning("support_list_cases_invalid_rows type=%s", type(rows).__name__)
            return []
        q = str(query_text or "").strip().lower()
        if q:
            rows = [
                x
                for x in rows
                if q in str(x.get("subject", "")).lower()
                or q in str(x.get("guild_name", "")).lower()
                or q in str(x.get("last_message_preview", "")).lower()
                or q in str(x.get("id", "")).lower()
            ]
        return rows

    def create_case(
        self,
        guild_id: str,
        guild_name: str,
        created_by_id: str,
        created_by_name: str,
        subject: str,
        priority: str,
        body: str,
        actor_role: str,
    ) -> int:
        """Create support case, seed first message, and emit realtime/audit side-effects."""
        now = int(time.time())
        case_id = self.repository.create_case(
            (str(guild_id), str(guild_name), str(created_by_id), str(created_by_name), str(subject), str(priority), now, now, now, str(body)[:220], now)
        )
        self.repository.add_initial_message((int(case_id), str(guild_id), str(created_by_id), str(created_by_name), str(actor_role), str(body), now))
        self.realtime_service.support_case_event(str(guild_id), int(case_id), "created")
        self.audit_callback(str(guild_id), "support_case_created", str(created_by_id), str(created_by_name), target_id=str(case_id), metadata={"priority": str(priority)})
        if str(priority) == "urgent":
            case_row = self.repository.get_case(int(case_id))
            if case_row:
                self.send_webhook_callback("Urgent case created", case_row, "Immediate support attention required.")
        return int(case_id)

    def list_messages(self, case_id: int, privileged: bool) -> list[dict]:
        """Return case messages with visibility constraints."""
        rows = self.repository.messages_for_case(int(case_id), bool(privileged))
        if not isinstance(rows, list):
            LOGGER.warning("support_list_messages_invalid_rows case_id=%s", case_id)
            return []
        return rows

    def add_message(
        self,
        case_row: dict,
        actor_id: str,
        actor_name: str,
        actor_role: str,
        body: str,
        visibility: str,
    ) -> None:
        """Append message to case and update status/SLA fields atomically."""
        case_id = int(case_row.get("id", 0) or 0)
        guild_id = str(case_row.get("guild_id", ""))
        now = int(time.time())
        self.repository.add_message((case_id, guild_id, str(actor_id), str(actor_name), str(actor_role), str(body), str(visibility), now))

        new_status = str(case_row.get("status", "open"))
        requester_last = int(case_row.get("requester_last_message_at", 0) or 0)
        support_last = int(case_row.get("support_last_message_at", 0) or 0)
        first_response = int(case_row.get("first_response_at", 0) or 0)
        resolved_at = int(case_row.get("resolved_at", 0) or 0)

        if actor_role in {"support_admin", "owner", "server_admin"} and visibility == "public":
            support_last = now
            if first_response <= 0:
                first_response = now
            if new_status == "open":
                new_status = "in_progress"

        if actor_role == "viewer" and visibility == "public":
            requester_last = now
            if new_status in {"resolved", "closed"}:
                new_status = "open"
                resolved_at = 0

        self.repository.update_case_after_message(
            (
                new_status,
                now,
                now,
                str(body)[:220],
                "requester" if actor_role == "viewer" else "support",
                requester_last,
                support_last,
                first_response,
                resolved_at,
                0 if actor_role == "viewer" else int(case_row.get("sla_alert_sent_at", 0) or 0),
                case_id,
            )
        )
        self.realtime_service.support_case_event(guild_id, case_id, "message")
        self.audit_callback(guild_id, "support_case_message_added", str(actor_id), str(actor_name), target_id=str(case_id), metadata={"visibility": str(visibility)})

    def assign_case(self, case_row: dict, action: str, actor_id: str, actor_name: str) -> None:
        """Assign or unassign support case and notify listeners."""
        now = int(time.time())
        case_id = int(case_row.get("id", 0) or 0)
        guild_id = str(case_row.get("guild_id", ""))
        self.repository.update_case_assignment(case_id=case_id, now=now, uid=actor_id, uname=actor_name, unassign=(str(action) == "unassign"))
        self.realtime_service.support_case_event(guild_id, case_id, "assignment")
        self.audit_callback(guild_id, "support_case_assignment", str(actor_id), str(actor_name), target_id=str(case_id), metadata={"action": str(action)})

    def update_status(self, case_row: dict, status: str, actor_id: str, actor_name: str) -> None:
        """Change support case status and publish update events."""
        now = int(time.time())
        resolved_at = now if status in {"resolved", "closed"} else 0
        case_id = int(case_row.get("id", 0) or 0)
        guild_id = str(case_row.get("guild_id", ""))
        self.repository.update_case_status(case_id=case_id, status=status, now=now, resolved_at=resolved_at)
        self.realtime_service.support_case_event(guild_id, case_id, "status")
        self.audit_callback(guild_id, "support_case_status_changed", str(actor_id), str(actor_name), target_id=str(case_id), metadata={"status": str(status)})

    def maybe_send_sla_alert(self, case_row: dict, is_breached_callback) -> None:
        """Send SLA alert once per cooldown window when breach criteria are met."""
        if not case_row:
            return
        now = int(time.time())
        if not is_breached_callback(case_row, now):
            return
        last_alert = int(case_row.get("sla_alert_sent_at", 0) or 0)
        if last_alert and (now - last_alert) < 30 * 60:
            return
        self.send_webhook_callback("SLA breached", case_row, "Case is waiting on support response.")
        self.repository.mark_sla_alert(int(case_row.get("id", 0) or 0), now)
