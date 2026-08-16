import os
import threading
import time
from collections import defaultdict, deque


class TTLCache:
    def __init__(self, max_entries: int = 4096):
        self._data = {}
        self._lock = threading.RLock()
        self._max_entries = max(int(max_entries), 16)
        self._writes = 0

    def _prune_locked(self, now: float):
        expired = [key for key, item in self._data.items() if item[0] <= now]
        for key in expired:
            self._data.pop(key, None)
        overflow = len(self._data) - self._max_entries
        # Python dicts preserve insertion order. FIFO eviction is sufficient for
        # these short-lived caches and avoids sorting thousands of entries on
        # every new key after the cache reaches its limit.
        for _ in range(max(overflow, 0)):
            if not self._data:
                break
            self._data.pop(next(iter(self._data)), None)

    def get(self, key, default=None):
        now = time.monotonic()
        with self._lock:
            item = self._data.get(key)
            if not item:
                return default
            expires_at, value = item
            if expires_at <= now:
                self._data.pop(key, None)
                return default
            return value

    def set(self, key, value, ttl_seconds: float):
        with self._lock:
            now = time.monotonic()
            self._data[key] = (now + max(float(ttl_seconds), 0.1), value)
            self._writes += 1
            if len(self._data) > self._max_entries or self._writes % 256 == 0:
                self._prune_locked(now)
        return value

    def invalidate(self, key=None):
        with self._lock:
            if key is None:
                self._data.clear()
            else:
                self._data.pop(key, None)


class CallbackRateLimiter:
    def __init__(self, window_seconds: float, max_actions: int, duplicate_cooldown: float,
                 max_users: int = 8192):
        self.window = max(float(window_seconds), 1.0)
        self.max_actions = max(int(max_actions), 2)
        self.duplicate_cooldown = max(float(duplicate_cooldown), 0.0)
        self.max_users = max(int(max_users), 128)
        self._events = defaultdict(deque)
        self._last_callback = {}
        self._lock = threading.RLock()
        self._calls = 0

    def _prune_locked(self, now: float):
        cutoff = now - self.window
        idle_cutoff = now - max(self.window, self.duplicate_cooldown, 1.0)
        for uid, events in list(self._events.items()):
            while events and events[0] < cutoff:
                events.popleft()
            previous = self._last_callback.get(uid)
            if not events and (not previous or previous[1] < idle_cutoff):
                self._events.pop(uid, None)
                self._last_callback.pop(uid, None)

    def allow(self, user_id: int, callback: str):
        now = time.monotonic()
        uid = int(user_id)
        callback = str(callback or "")
        with self._lock:
            self._calls += 1
            if self._calls % 256 == 0:
                self._prune_locked(now)
            previous = self._last_callback.get(uid)
            if previous and previous[0] == callback and now - previous[1] < self.duplicate_cooldown:
                return False, "درخواست تکراری بود؛ یک لحظه صبر کنید."
            q = self._events[uid]
            cutoff = now - self.window
            while q and q[0] < cutoff:
                q.popleft()
            if len(q) >= self.max_actions:
                return False, "تعداد درخواست‌ها زیاد است؛ چند ثانیه صبر کنید."
            q.append(now)
            self._last_callback[uid] = (callback, now)
            while len(self._events) > self.max_users:
                oldest_uid = next(iter(self._events))
                self._events.pop(oldest_uid, None)
                self._last_callback.pop(oldest_uid, None)
            return True, ""

    def tracked_users(self) -> int:
        with self._lock:
            self._prune_locked(time.monotonic())
            return len(self._events)


class RuntimeMonitor:
    def __init__(self):
        now = time.monotonic()
        self._lock = threading.RLock()
        self.started_monotonic = now
        self.started_wall = time.time()
        self.last_heartbeat = now
        self.last_update = now
        self.total_updates = 0
        self.in_flight = 0
        self.slow_updates = deque(maxlen=50)
        self.slow_operations = deque(maxlen=100)
        self.service_health = {}
        self.shutting_down = False
        self.watchdog_started = False

    def heartbeat(self):
        with self._lock:
            self.last_heartbeat = time.monotonic()

    def update_started(self):
        with self._lock:
            self.last_update = time.monotonic()
            self.total_updates += 1
            self.in_flight += 1

    def update_finished(self, *, elapsed: float, tg_id: int, callback: str):
        with self._lock:
            self.in_flight = max(self.in_flight - 1, 0)
            self.last_update = time.monotonic()
            if elapsed >= 1.0:
                self.slow_updates.append({"seconds": round(elapsed, 3), "tg_id": int(tg_id), "callback": str(callback or "")[:120], "at": time.time()})

    def operation(self, name: str, elapsed: float, ok: bool):
        if elapsed < 0.25:
            return
        with self._lock:
            self.slow_operations.append({"name": str(name)[:100], "seconds": round(elapsed, 3), "ok": bool(ok), "at": time.time()})

    def set_service_health(self, name: str, ok: bool, detail: str = "", elapsed: float = 0.0):
        with self._lock:
            self.service_health[str(name)] = {"ok": bool(ok), "detail": str(detail)[:300], "seconds": round(float(elapsed), 3), "at": time.time()}

    def snapshot(self):
        now = time.monotonic()
        with self._lock:
            return {
                "uptime_seconds": max(now - self.started_monotonic, 0),
                "heartbeat_age_seconds": max(now - self.last_heartbeat, 0),
                "last_update_age_seconds": max(now - self.last_update, 0),
                "total_updates": int(self.total_updates),
                "in_flight": int(self.in_flight),
                "slow_updates": list(self.slow_updates)[-10:],
                "slow_operations": list(self.slow_operations)[-10:],
                "service_health": dict(self.service_health),
            }

    def start_watchdog(self, stale_seconds: float, *, logger=None):
        with self._lock:
            if self.watchdog_started:
                return
            self.watchdog_started = True
        stale_seconds = max(float(stale_seconds), 30.0)

        def loop():
            # The watchdog is an independent OS thread. If asyncio truly freezes,
            # it can terminate only the Python worker; systemd then restarts it.
            while True:
                time.sleep(min(10.0, stale_seconds / 3))
                with self._lock:
                    if self.shutting_down:
                        return
                    age = time.monotonic() - self.last_heartbeat
                if age > stale_seconds:
                    if logger:
                        logger.critical("WATCHDOG: asyncio heartbeat stale for %.1fs; restarting bot worker", age)
                    os._exit(75)

        threading.Thread(target=loop, name="bot-watchdog", daemon=True).start()

    def stop_watchdog(self):
        with self._lock:
            self.shutting_down = True


STATUS_CACHE = TTLCache()
RUNTIME = RuntimeMonitor()
