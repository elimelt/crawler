from crawlerlib.config import CrawlConfig
from crawlerlib.engine import Crawler
from crawlerlib.types import FetchResult, HttpClientProtocol


class StubHttp(HttpClientProtocol):
    def fetch(self, url: str) -> FetchResult | None:
        if url.endswith("/a"):
            html = '<html><head><title>A</title></head><body><a href="/b">b</a></body></html>'
            return FetchResult(status=200, content_type="text/html", text=html, size_bytes=len(html.encode()))
        if url.endswith("/b"):
            html = "<html><head><title>B</title></head><body>done</body></html>"
            return FetchResult(status=200, content_type="text/html", text=html, size_bytes=len(html.encode()))
        return FetchResult(status=404, content_type="text/html", text="", size_bytes=0)


def test_engine_walks_links(tmp_path):
    cfg = CrawlConfig(
        start_urls=["https://example.com/a"],
        allowed_domains=["example.com"],
        max_pages=10,
        max_depth=2,
        concurrency=2,
        output_path=str(tmp_path / "out.jsonl"),
        obey_robots_txt=False,
        metrics_interval=0.0,
    )
    c = Crawler(cfg, http_client=StubHttp())
    c.run()
    # Expect at least pages crawled for /a and /b
    assert c.pages_crawled >= 2

