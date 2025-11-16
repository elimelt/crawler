import logging
import threading
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from .metrics import Metrics


logger = logging.getLogger(__name__)


class PrometheusExporter:
    def __init__(self, metrics: Metrics, port: int = 8000) -> None:
        self.metrics = metrics
        self.port = port
        self._server_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

        self.pages_total = Counter('crawler_pages_total', 'Total number of pages crawled')
        self.bytes_total = Counter('crawler_bytes_total', 'Total number of bytes downloaded')
        self.errors_total = Counter('crawler_errors_total', 'Total number of crawl errors')
        self.fetch_duration_seconds = Histogram(
            'crawler_fetch_duration_seconds',
            'HTTP fetch duration in seconds',
            buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        )
        self.pages_per_second = Gauge('crawler_pages_per_second', 'Current crawl rate in pages per second')
        self.avg_fetch_duration_seconds = Gauge('crawler_avg_fetch_duration_seconds', 'Average fetch duration in seconds')

        self._last_pages = 0
        self._last_bytes = 0
        self._last_errors = 0
        
    def start(self) -> None:
        start_http_server(self.port)
        logger.info(f"Prometheus metrics server started on port {self.port}")

        self._server_thread = threading.Thread(
            target=self._update_metrics_loop,
            name="prometheus-updater",
            daemon=True
        )
        self._server_thread.start()
    
    def _update_metrics_loop(self) -> None:
        while not self._stop_event.is_set():
            self._update_metrics()
            self._stop_event.wait(5.0)

    def _update_metrics(self) -> None:
        totals, elapsed = self.metrics.snapshot()

        pages_delta = totals.pages - self._last_pages
        bytes_delta = totals.bytes - self._last_bytes
        errors_delta = totals.errors - self._last_errors

        if pages_delta > 0:
            self.pages_total.inc(pages_delta)
        if bytes_delta > 0:
            self.bytes_total.inc(bytes_delta)
        if errors_delta > 0:
            self.errors_total.inc(errors_delta)

        if elapsed > 0:
            pps = totals.pages / elapsed
            self.pages_per_second.set(pps)

        if totals.pages > 0:
            avg_fetch_ms = totals.fetch_ms_sum / totals.pages
            self.avg_fetch_duration_seconds.set(avg_fetch_ms / 1000.0)

        self._last_pages = totals.pages
        self._last_bytes = totals.bytes
        self._last_errors = totals.errors

    def stop(self) -> None:
        self._stop_event.set()
        if self._server_thread:
            self._server_thread.join(timeout=2.0)

