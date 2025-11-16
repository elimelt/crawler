import threading
import time
from typing import Dict, Callable


class RateLimiter:
    def __init__(self, delay_seconds: float, now: Callable[[], float] | None = None, sleep: Callable[[float], None] | None = None):
        self.delay_seconds = delay_seconds
        self._host_next_time: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._now = now or time.time
        self._sleep = sleep or time.sleep

    def wait_turn(self, netloc: str) -> None:
        if self.delay_seconds <= 0:
            return
        with self._lock:
            now = self._now()
            next_allowed = self._host_next_time.get(netloc, 0.0)
            if next_allowed > now:
                sleep_for = next_allowed - now
            else:
                sleep_for = 0.0
            self._host_next_time[netloc] = max(next_allowed, now) + self.delay_seconds
        if sleep_for > 0:
            self._sleep(sleep_for)

