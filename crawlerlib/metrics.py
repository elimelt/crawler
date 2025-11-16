import threading
import time
from dataclasses import dataclass


@dataclass
class Totals:
    pages: int = 0
    bytes: int = 0
    errors: int = 0
    fetch_ms_sum: float = 0.0


class Metrics:
    def __init__(self):
        self._totals = Totals()
        self._lock = threading.Lock()
        self._start = time.time()

    def record_fetch(self, ok: bool, bytes_read: int, fetch_ms: float) -> None:
        with self._lock:
            self._totals.pages += 1
            self._totals.bytes += max(0, bytes_read)
            if not ok:
                self._totals.errors += 1
            self._totals.fetch_ms_sum += fetch_ms

    def snapshot(self) -> tuple[Totals, float]:
        with self._lock:
            t = Totals(
                pages=self._totals.pages,
                bytes=self._totals.bytes,
                errors=self._totals.errors,
                fetch_ms_sum=self._totals.fetch_ms_sum,
            )
        elapsed = max(1e-6, time.time() - self._start)
        return t, elapsed


class StatsLogger(threading.Thread):
    daemon = True

    def __init__(self, metrics: Metrics, interval_s: float, log_fn):
        super().__init__(name="stats-logger")
        self._metrics = metrics
        self._interval = max(0.5, interval_s)
        self._log = log_fn
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            self._stop.wait(self._interval)
            if self._stop.is_set():
                break
            totals, elapsed = self._metrics.snapshot()
            pps = totals.pages / elapsed
            mb = totals.bytes / (1024 * 1024)
            avg_ms = (totals.fetch_ms_sum / max(1, totals.pages))
            self._log(
                "Perf: pages=%d, errors=%d, MB=%.2f, avg_fetch_ms=%.1f, pages/sec=%.2f",
                totals.pages,
                totals.errors,
                mb,
                avg_ms,
                pps,
            )

    def stop(self) -> None:
        self._stop.set()

