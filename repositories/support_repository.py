"""Support case/message repository."""

from __future__ import annotations


class SupportRepository:
    """Encapsulate support case and message queries."""

    def __init__(self, fetchall_sync, fetchone_sync, execute_sync):
        self._fetchall = fetchall_sync
        self._fetchone = fetchone_sync
        self._execute = execute_sync

    def get_case(self, case_id: int) -> dict | None:
        return self._fetchone("SELECT * FROM support_cases WHERE id=?", (int(case_id),))

    def query_cases(self, where_sql: str, params: tuple) -> list[dict]:
        sql = (
            """
            SELECT id, guild_id, guild_name, created_by_id, created_by_name, subject, priority, status,
                   assigned_to_id, assigned_to_name, created_at, updated_at, last_message_at, last_message_preview,
                   last_actor_role, requester_last_message_at, support_last_message_at, first_response_at, resolved_at, sla_alert_sent_at
            FROM support_cases
            """
            + where_sql
            + " ORDER BY updated_at DESC LIMIT 200"
        )
        return self._fetchall(sql, params)

    def create_case(self, payload: tuple) -> int:
        lastrowid, _ = self._execute(
            """
            INSERT INTO support_cases (
              guild_id, guild_name, created_by_id, created_by_name, subject, priority, status,
              created_at, updated_at, last_message_at, last_message_preview,
              last_actor_role, requester_last_message_at, support_last_message_at, first_response_at, resolved_at, sla_alert_sent_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, 'requester', ?, 0, 0, 0, 0)
            """,
            payload,
        )
        return int(lastrowid)

    def add_message(self, payload: tuple) -> None:
        self._execute(
            """
            INSERT INTO support_messages (case_id, guild_id, author_id, author_name, author_role, body, visibility, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )

    def add_initial_message(self, payload: tuple) -> None:
        self._execute(
            """
            INSERT INTO support_messages (case_id, guild_id, author_id, author_name, author_role, body, visibility, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 'public', ?)
            """,
            payload,
        )

    def messages_for_case(self, case_id: int, privileged: bool) -> list[dict]:
        if privileged:
            return self._fetchall(
                """
                SELECT id, case_id, guild_id, author_id, author_name, author_role, body, visibility, created_at
                FROM support_messages
                WHERE case_id=?
                ORDER BY created_at ASC, id ASC
                LIMIT 500
                """,
                (int(case_id),),
            )
        return self._fetchall(
            """
            SELECT id, case_id, guild_id, author_id, author_name, author_role, body, visibility, created_at
            FROM support_messages
            WHERE case_id=? AND visibility='public'
            ORDER BY created_at ASC, id ASC
            LIMIT 500
            """,
            (int(case_id),),
        )

    def update_case_after_message(self, payload: tuple) -> None:
        self._execute(
            """
            UPDATE support_cases
            SET status=?, updated_at=?, last_message_at=?, last_message_preview=?, last_actor_role=?,
                requester_last_message_at=?, support_last_message_at=?, first_response_at=?, resolved_at=?, sla_alert_sent_at=?
            WHERE id=?
            """,
            payload,
        )

    def update_case_assignment(self, case_id: int, now: int, uid: str, uname: str, unassign: bool) -> None:
        if unassign:
            self._execute(
                "UPDATE support_cases SET assigned_to_id='', assigned_to_name='', updated_at=? WHERE id=?",
                (int(now), int(case_id)),
            )
            return
        self._execute(
            "UPDATE support_cases SET assigned_to_id=?, assigned_to_name=?, status='in_progress', updated_at=? WHERE id=?",
            (str(uid), str(uname), int(now), int(case_id)),
        )

    def update_case_status(self, case_id: int, status: str, now: int, resolved_at: int) -> None:
        self._execute(
            "UPDATE support_cases SET status=?, updated_at=?, resolved_at=? WHERE id=?",
            (str(status), int(now), int(resolved_at), int(case_id)),
        )

    def mark_sla_alert(self, case_id: int, now: int) -> None:
        self._execute("UPDATE support_cases SET sla_alert_sent_at=?, updated_at=? WHERE id=?", (int(now), int(now), int(case_id)))
