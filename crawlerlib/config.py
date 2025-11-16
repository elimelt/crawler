from dataclasses import dataclass
from typing import List, Optional


DEFAULT_USER_AGENT = "useful-crawler/2.0 (+https://example.com; contact: crawler@example.com)"


@dataclass(frozen=True)
class CrawlConfig:
    start_urls: List[str]
    allowed_domains: List[str]
    max_pages: int = 200
    max_depth: int = 2
    concurrency: int = 8
    max_connections: int = 16
    delay_seconds: float = 0.5
    request_timeout: float = 15.0
    user_agent: str = DEFAULT_USER_AGENT
    obey_robots_txt: bool = True
    output_path: str = "crawl.jsonl"
    sqlite_path: Optional[str] = None
    resume: bool = False
    metrics_interval: float = 10.0

