import logging
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse
from pathlib import Path

from .config import CrawlConfig
from .net import HttpClient, RobotsCache
from .parsing import Extractor, UrlTools
from .rate import RateLimiter
from .storage import JsonlWriter, SqliteStore
from .metrics import Metrics, StatsLogger
from .types import HttpClientProtocol, FetchResult


class Crawler:
    def __init__(self, config: CrawlConfig, http_client: HttpClientProtocol | None = None):
        self.config = config
        self.http = http_client or HttpClient(
            config.user_agent, config.request_timeout, config.concurrency, config.max_connections
        )
        self.robots = RobotsCache(config.user_agent, self.http)
        self.rate = RateLimiter(config.delay_seconds)
        self.frontier: "queue.Queue[Tuple[str,int]]" = queue.Queue()
        self.visited: Set[str] = set()
        self.visited_lock = threading.Lock()
        self.pages_crawled = 0
        self.pages_lock = threading.Lock()
        resume_from_output = Path(config.output_path).exists()
        self.writer = JsonlWriter(config.output_path, append=(config.resume or resume_from_output))
        self.allowed_domains = [d.lower().lstrip(".") for d in config.allowed_domains]
        self.store: Optional[SqliteStore] = SqliteStore(config.sqlite_path) if config.sqlite_path else None
        self.metrics = Metrics()
        self.stats_thread: Optional[StatsLogger] = None
        self._bloom = None

        starts = UrlTools.normalize_start(config.start_urls)
        should_resume = self.store is not None and (self.config.resume or resume_from_output)
        if self.store and should_resume:
            # Pre-populate bloom (if available) from SQLite to ensure exact resume semantics.
            try:
                from .bloom_filter import create_bloom_filter
                self._bloom = create_bloom_filter(expected_urls=max(self.config.max_pages, 1))
                # Stream only visited pages from SQLite to avoid false positives on frontier
                chunk: List[str] = []
                chunk_size = 10_000
                for url in self.store.iter_pages_urls(batch_size=chunk_size):
                    chunk.append(url)
                    if len(chunk) >= chunk_size:
                        self._bloom.add_batch(chunk)
                        chunk.clear()
                if chunk:
                    self._bloom.add_batch(chunk)
            except Exception:
                self._bloom = None
            restored = self.store.load_frontier()
            if restored:
                for url, depth in restored:
                    self.frontier.put((url, depth))
            else:
                for url in starts:
                    self.frontier.put((url, 0))
                    self._persist_enqueue(url, 0)
        else:
            for url in starts:
                self.frontier.put((url, 0))
                self._persist_enqueue(url, 0)

    def _persist_enqueue(self, url: str, depth: int) -> bool:
        if not self.store:
            return True
        return self.store.mark_enqueued(url, depth)

    def _within_limit(self) -> bool:
        with self.pages_lock:
            return self.pages_crawled < self.config.max_pages

    def _increment_pages(self) -> None:
        with self.pages_lock:
            self.pages_crawled += 1

    def _should_visit(self, url: str) -> bool:
        if not UrlTools.is_allowed_domain(url, self.allowed_domains):
            return False
        # If using SQLite, prefer Bloom filter with SQLite fallback; otherwise use in-memory set.
        if self.store:
            # Lazy-init Bloom filter sized to expected crawl; fall back to defaults if needed.
            if not hasattr(self, "_bloom") or self._bloom is None:
                try:
                    from .bloom_filter import create_bloom_filter
                    expected = max(self.config.max_pages, 1)
                    self._bloom = create_bloom_filter(expected_urls=expected)
                except Exception:
                    # Very defensive: if anything goes wrong, disable Bloom usage for this run.
                    self._bloom = None
            # Fast path via Bloom filter; guard with lock for thread-safety.
            if self._bloom is not None:
                with self.visited_lock:
                    if not self._bloom.contains(url):
                        self._bloom.add(url)
                        return True
            # Possible Bloom positive; confirm against SQLite pages to avoid skipping uncrawled URLs.
            if self.store.has_page(url):
                return False
            # Not present in SQLite; accept and ensure Bloom records it for subsequent checks.
            if self._bloom is not None:
                with self.visited_lock:
                    self._bloom.add(url)
            return True
        else:
            with self.visited_lock:
                if url in self.visited:
                    return False
                self.visited.add(url)
            return True

    def _enqueue_links(self, links: Iterable[str], current_depth: int) -> None:
        next_depth = current_depth + 1
        if next_depth > self.config.max_depth:
            return
        for link in links:
            if UrlTools.is_allowed_domain(link, self.allowed_domains):
                if self.store:
                    if not self.store.seen_url(link) and self._persist_enqueue(link, next_depth):
                        self.frontier.put((link, next_depth))
                else:
                    self.frontier.put((link, next_depth))

    def worker(self) -> None:
        while self._within_limit():
            try:
                url, depth = self.frontier.get(timeout=0.5)
            except queue.Empty:
                return
            if not self._within_limit():
                self.frontier.task_done()
                return
            if self.config.obey_robots_txt and not self.robots.can_fetch(url):
                logging.debug("Disallowed by robots.txt: %s", url)
                self.frontier.task_done()
                if self.store:
                    self.store.dequeue(url)
                continue
            if not self._should_visit(url):
                self.frontier.task_done()
                continue
            # politeness
            parsed = urlparse(url)
            self.rate.wait_turn(parsed.netloc)

            import time as _t
            t0 = _t.perf_counter()
            response = self.http.fetch(url)
            dt_ms = (_t.perf_counter() - t0) * 1000.0
            if response is None:
                self.frontier.task_done()
                if self.store:
                    self.store.dequeue(url)
                self.metrics.record_fetch(False, 0, dt_ms)
                continue
            status, content_type, text, size_bytes = response.status, response.content_type, response.text, response.size_bytes
            record: Dict = {"url": url, "status": status, "content_type": content_type}
            if text and "text/html" in content_type:
                extracted, links = Extractor.extract(url, text)
                record.update(extracted)
                self._enqueue_links(links, depth)
                if self.store:
                    self.store.add_links(url, links)
            else:
                record["num_links"] = 0
                record["title"] = ""
                record["description"] = ""
                record["text"] = ""

            self.writer.write(record)
            self.metrics.record_fetch(True, size_bytes, dt_ms)
            if self.store:
                self.store.save_page(record, depth)
                self.store.dequeue(url)
            self._increment_pages()
            if self.pages_crawled % 10 == 0:
                logging.info("Crawled %d pages", self.pages_crawled)
            self.frontier.task_done()

    def run(self) -> None:
        logging.info(
            "Starting crawl: %d start URLs, allowed domains: %s",
            len(self.config.start_urls),
            ", ".join(self.allowed_domains) or "(none)",
        )
        if self.config.metrics_interval and self.config.metrics_interval > 0:
            self.stats_thread = StatsLogger(self.metrics, self.config.metrics_interval, logging.info)
            self.stats_thread.start()
        with ThreadPoolExecutor(max_workers=self.config.concurrency) as executor:
            futures = [executor.submit(self.worker) for _ in range(self.config.concurrency)]
            for _ in as_completed(futures):
                pass
        if self.stats_thread:
            self.stats_thread.stop()
        logging.info("Finished. Pages crawled: %d. Output: %s", self.pages_crawled, self.config.output_path)

