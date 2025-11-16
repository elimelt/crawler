from typing import Optional, Tuple
from urllib import robotparser
from urllib.parse import urljoin, urlparse

import urllib3
from urllib3.util.retry import Retry
from urllib3 import exceptions as urllib3_exc

from .types import FetchResult


class HttpClient:
    def __init__(self, user_agent: str, request_timeout: float, concurrency: int, max_connections: int = 16):
        self.user_agent = user_agent
        self.timeout = urllib3.Timeout(connect=5.0, read=request_timeout)
        self.http = urllib3.PoolManager(
            num_pools=max(8, concurrency),
            maxsize=max_connections,
            headers={
                "User-Agent": user_agent,
                "Accept": "text/html,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate",
            },
            retries=Retry(
                total=2,
                backoff_factor=0.3,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET", "HEAD"],
                raise_on_status=False,
            ),
        )

    def _request_bytes(self, url: str) -> Optional[Tuple[int, str, bytes]]:
        try:
            response = self.http.request(
                "GET",
                url,
                timeout=self.timeout,
                preload_content=True,
                headers={"User-Agent": self.user_agent},
            )
        except urllib3_exc.HTTPError:
            return None
        return response.status, response.headers.get("Content-Type", ""), response.data or b""

    def fetch(self, url: str) -> Optional[FetchResult]:
        res = self._request_bytes(url)
        if res is None:
            return None
        status, content_type, body = res
        size_bytes = len(body)
        text = ""
        if "text/html" in (content_type or "") or "text/plain" in (content_type or ""):
            try:
                text = body.decode("utf-8", errors="ignore")
            except Exception:
                text = ""
        return FetchResult(status=status, content_type=content_type or "", text=text, size_bytes=size_bytes)


class RobotsCache:
    def __init__(self, user_agent: str, http: HttpClient):
        self.user_agent = user_agent
        self.http = http
        self._cache: dict[str, Optional[robotparser.RobotFileParser]] = {}

    def _fetch_robots(self, root: str) -> Optional[robotparser.RobotFileParser]:
        robots_url = urljoin(root, "/robots.txt")
        response = self.http._request_bytes(robots_url)
        if not response:
            return None
        status, content_type, body = response
        if status >= 400:
            return None
        rp = robotparser.RobotFileParser()
        rp.set_url(robots_url)
        try:
            txt = body.decode("utf-8", errors="ignore")
        except Exception:
            txt = ""
        rp.parse(txt.splitlines())
        return rp

    def can_fetch(self, url: str) -> bool:
        parsed = urlparse(url)
        root = f"{parsed.scheme}://{parsed.netloc}"
        if root not in self._cache:
            self._cache[root] = self._fetch_robots(root)
        rp = self._cache[root]
        if rp is None:
            return True
        return rp.can_fetch(self.user_agent, url)

