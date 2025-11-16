from pathlib import Path
import json

from crawlerlib.config import CrawlConfig
from crawlerlib.engine import Crawler
from crawlerlib.types import FetchResult, HttpClientProtocol
from crawlerlib.storage import SqliteStore


class StubHttpResume(HttpClientProtocol):
    def fetch(self, url: str) -> FetchResult | None:
        if url.endswith("/a"):
            html = '<html><head><title>A</title></head><body><a href="/b">b</a></body></html>'
            return FetchResult(status=200, content_type="text/html", text=html, size_bytes=len(html.encode()))
        if url.endswith("/b"):
            html = "<html><head><title>B</title></head><body>done</body></html>"
            return FetchResult(status=200, content_type="text/html", text=html, size_bytes=len(html.encode()))
        return FetchResult(status=404, content_type="text/html", text="", size_bytes=0)


def read_jsonl(path: Path):
    lines = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                lines.append(json.loads(line))
    return lines


def test_sqlite_resumability_frontier_and_append(tmp_path):
    out = tmp_path / "out.jsonl"
    db = tmp_path / "crawl.db"
    start = "https://example.com/a"

    # First run: stop after one page to simulate interruption
    cfg1 = CrawlConfig(
        start_urls=[start],
        allowed_domains=["example.com"],
        max_pages=1,
        max_depth=2,
        concurrency=1,
        output_path=str(out),
        sqlite_path=str(db),
        obey_robots_txt=False,
        metrics_interval=0.0,
        resume=False,
    )
    c1 = Crawler(cfg1, http_client=StubHttpResume())
    c1.run()

    # Validate first run outputs
    assert out.exists()
    lines1 = read_jsonl(out)
    assert len(lines1) == 1
    assert lines1[0]["url"].endswith("/a")

    # DB should have one page and frontier should contain /b (enqueued but not crawled)
    store = SqliteStore(str(db))
    known = set(store.all_known_urls())
    assert any(u.endswith("/a") for u in known)
    assert any(u.endswith("/b") for u in known)  # frontier entry

    # Second run: resume; should crawl /b and append to JSONL without recrawling /a
    cfg2 = CrawlConfig(
        start_urls=[start],  # ignored due to resume
        allowed_domains=["example.com"],
        max_pages=10,
        max_depth=2,
        concurrency=1,
        output_path=str(out),
        sqlite_path=str(db),
        obey_robots_txt=False,
        metrics_interval=0.0,
        resume=True,
    )
    c2 = Crawler(cfg2, http_client=StubHttpResume())
    c2.run()

    lines2 = read_jsonl(out)
    urls2 = [rec["url"] for rec in lines2]
    assert any(u.endswith("/b") for u in urls2)
    # ensure /a wasn't re-crawled (only one occurrence)
    assert urls2.count(start) == 1


class ChainHttp(HttpClientProtocol):
    def __init__(self, total: int):
        self.total = total

    def fetch(self, url: str) -> FetchResult | None:
        # Expect URLs like https://example.com/page/{i}
        try:
            idx_str = url.rstrip("/").split("/")[-1]
            i = int(idx_str)
        except Exception:
            return FetchResult(status=404, content_type="text/html", text="", size_bytes=0)
        if i < 0 or i >= self.total:
            return FetchResult(status=404, content_type="text/html", text="", size_bytes=0)
        links = []
        if i + 1 < self.total:
            links.append(f"/page/{i+1}")
        if i + 2 < self.total:
            links.append(f"/page/{i+2}")
        body = "<html><head><title>P{}</title></head><body>{}</body></html>".format(
            i, "".join(f'<a href="{h}">n</a>' for h in links)
        )
        return FetchResult(status=200, content_type="text/html", text=body, size_bytes=len(body.encode()))


def mk_url(i: int) -> str:
    return f"https://example.com/page/{i}"


def test_sqlite_resumability_large_streaming_preload(tmp_path):
    out = tmp_path / "out_large.jsonl"
    db = tmp_path / "crawl_large.db"
    total = 800
    first_batch = 300

    cfg1 = CrawlConfig(
        start_urls=[mk_url(0)],
        allowed_domains=["example.com"],
        max_pages=first_batch,
        max_depth=total,
        concurrency=1,
        delay_seconds=0.0,
        output_path=str(out),
        sqlite_path=str(db),
        obey_robots_txt=False,
        metrics_interval=0.0,
        resume=False,
    )
    c1 = Crawler(cfg1, http_client=ChainHttp(total))
    c1.run()
    lines1 = read_jsonl(out)
    assert len(lines1) == first_batch

    # Ensure frontier was populated (so resume path uses restored frontier)
    store = SqliteStore(str(db))
    restored = store.load_frontier()
    assert len(restored) > 0

    cfg2 = CrawlConfig(
        start_urls=[mk_url(0)],
        allowed_domains=["example.com"],
        max_pages=total,
        max_depth=total,
        concurrency=1,
        delay_seconds=0.0,
        output_path=str(out),
        sqlite_path=str(db),
        obey_robots_txt=False,
        metrics_interval=0.0,
        resume=True,
    )
    c2 = Crawler(cfg2, http_client=ChainHttp(total))
    c2.run()

    lines2 = read_jsonl(out)
    urls2 = [rec["url"] for rec in lines2]
    assert len(urls2) == total
    assert len(set(urls2)) == total  # no duplicates after resume
    # spot check presence of some URLs across the range
    assert mk_url(0) in urls2
    assert mk_url(total - 1) in urls2


def test_sqlite_resume_bloom_fallback_on_visited_start(tmp_path):
    out = tmp_path / "out_fallback.jsonl"
    db = tmp_path / "crawl_fallback.db"
    total = 200
    first_batch = 100

    # First run
    cfg1 = CrawlConfig(
        start_urls=[mk_url(0)],
        allowed_domains=["example.com"],
        max_pages=first_batch,
        max_depth=total,
        concurrency=1,
        delay_seconds=0.0,
        output_path=str(out),
        sqlite_path=str(db),
        obey_robots_txt=False,
        metrics_interval=0.0,
        resume=False,
    )
    c1 = Crawler(cfg1, http_client=ChainHttp(total))
    c1.run()
    lines1 = read_jsonl(out)
    assert len(lines1) == first_batch

    # Clear frontier to force starts to be re-enqueued on resume (exercise Bloom positive + has_page)
    store = SqliteStore(str(db))
    for url, _depth in store.load_frontier():
        store.dequeue(url)
    assert len(store.load_frontier()) == 0

    # Second run: include visited start (0) and a new start (150)
    cfg2 = CrawlConfig(
        start_urls=[mk_url(0), mk_url(150)],
        allowed_domains=["example.com"],
        max_pages=total,
        max_depth=total,
        concurrency=1,
        delay_seconds=0.0,
        output_path=str(out),
        sqlite_path=str(db),
        obey_robots_txt=False,
        metrics_interval=0.0,
        resume=True,
    )
    c2 = Crawler(cfg2, http_client=ChainHttp(total))
    c2.run()

    lines2 = read_jsonl(out)
    urls2 = [rec["url"] for rec in lines2]
    # Should not re-crawl page 0; exactly one occurrence overall
    assert urls2.count(mk_url(0)) == 1
    # New start should be crawled
    assert mk_url(150) in urls2
    # We should have progressed beyond the first batch
    assert len(urls2) > first_batch
    assert len(set(urls2)) == len(urls2)


