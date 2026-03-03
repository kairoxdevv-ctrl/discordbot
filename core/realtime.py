import json
import logging
import queue
import threading

LOGGER = logging.getLogger("discordbot.realtime")


class RealtimeBus:
    def __init__(self):
        self.lock = threading.RLock()
        self.subscribers = {}

    def subscribe(self, key):
        q = queue.Queue(maxsize=128)
        with self.lock:
            self.subscribers.setdefault(key, []).append(q)
        return q

    def unsubscribe(self, key, q):
        with self.lock:
            lst = self.subscribers.get(key, [])
            if q in lst:
                lst.remove(q)

    def publish(self, key, payload):
        with self.lock:
            queues = list(self.subscribers.get(key, []))
        raw = json.dumps(payload, ensure_ascii=False)
        for q in queues:
            try:
                q.put_nowait(raw)
            except queue.Full:
                try:
                    q.get_nowait()
                    q.put_nowait(raw)
                except Exception:
                    LOGGER.warning("realtime_publish_drop key=%s", key, exc_info=True)
            except Exception:
                LOGGER.warning("realtime_publish_failed key=%s", key, exc_info=True)
