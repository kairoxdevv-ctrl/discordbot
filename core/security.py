"""Core layer: request security helpers (rate limiting, CSRF, sanitization)."""

import secrets
import threading
import time
from collections import defaultdict, deque


class RateLimiter:
    """In-memory HTTP request limiter keyed by IP/session/route.

    Responsibilities:
    - Enforce per-window request caps.
    - Periodically clean stale counters.
    Not responsible for auth/permission decisions.
    """

    def __init__(self):
        self.ip_hits = defaultdict(deque)
        self.session_hits = defaultdict(deque)
        self.route_hits = defaultdict(deque)
        self._last_cleanup = 0.0
        self._lock = threading.RLock()

    def _cleanup_bucket(self, bucket, window_sec, now):
        cutoff = now - window_sec
        stale = []
        for key, q in bucket.items():
            while q and q[0] < cutoff:
                q.popleft()
            if not q:
                stale.append(key)
        for key in stale:
            bucket.pop(key, None)

    def _hit(self, bucket, key, window_sec, limit):
        now = time.time()
        q = bucket[key]
        q.append(now)
        cutoff = now - window_sec
        while q and q[0] < cutoff:
            q.popleft()
        if not q:
            bucket.pop(key, None)
        return len(q) <= limit

    def allow(self, ip, session_id, route):
        """Return True when request is inside route/session/ip rate budgets."""
        with self._lock:
            now = time.time()
            if (now - self._last_cleanup) > 300:
                self._cleanup_bucket(self.ip_hits, 60, now)
                self._cleanup_bucket(self.session_hits, 60, now)
                self._cleanup_bucket(self.route_hits, 60, now)
                self._last_cleanup = now
            ip_ok = self._hit(self.ip_hits, ip, 60, 180)
            sess_ok = self._hit(self.session_hits, session_id or "anon", 60, 300)
            route_ok = self._hit(self.route_hits, route, 60, 240)
            return ip_ok and sess_ok and route_ok


def issue_csrf(session):
    token = secrets.token_urlsafe(32)
    session["csrf_token"] = token
    session["csrf_expires"] = int(time.time()) + 3600
    return token


def validate_csrf(session, token):
    if not token:
        return False
    expected = session.get("csrf_token")
    exp = int(session.get("csrf_expires", 0))
    if not expected or int(time.time()) > exp:
        return False
    return secrets.compare_digest(expected, token)


def sanitize_text(value: str, max_len: int):
    s = str(value or "").strip().replace("\x00", "")
    return s[:max_len]


def sanitize_id(value: str):
    s = str(value or "").strip()
    return s if s.isdigit() else ""
