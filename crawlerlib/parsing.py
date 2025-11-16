from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urldefrag, urlparse

from bs4 import BeautifulSoup


class UrlTools:
    @staticmethod
    def normalize_start(urls: Iterable[str]) -> List[str]:
        normalized: List[str] = []
        for u in urls:
            if not u:
                continue
            parsed = urlparse(u)
            if not parsed.scheme:
                u = "https://" + u
            u, _ = urldefrag(u)
            normalized.append(u)
        return normalized

    @staticmethod
    def normalize_link(base_url: str, href: str) -> Optional[str]:
        if not href:
            return None
        href = href.strip()
        if href.startswith(("mailto:", "javascript:", "tel:", "#")):
            return None
        absolute = urljoin(base_url, href)
        absolute, _ = urldefrag(absolute)
        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https"):
            return None
        return absolute

    @staticmethod
    def is_allowed_domain(url: str, allowed_domains: List[str]) -> bool:
        if not allowed_domains:
            return True
        host = urlparse(url).netloc.lower()
        return any(host == d or host.endswith("." + d) for d in allowed_domains)


class Extractor:
    @staticmethod
    def extract(url: str, html: str) -> Tuple[Dict, List[str]]:
        soup = BeautifulSoup(html, "html.parser")
        title_el = soup.find("title")
        title = title_el.get_text(strip=True) if title_el else ""
        desc_el = soup.find("meta", attrs={"name": "description"})
        if not desc_el or not desc_el.get("content"):
            desc_el = soup.find("meta", attrs={"property": "og:description"})
        description = (desc_el.get("content") or "").strip() if desc_el else ""
        text = soup.get_text(" ", strip=True)
        if len(text) > 4000:
            text = text[:4000]
        links: List[str] = []
        for a in soup.find_all("a", href=True):
            normalized = UrlTools.normalize_link(url, a["href"])
            if normalized:
                links.append(normalized)
        record = {
            "url": url,
            "title": title,
            "description": description,
            "text": text,
            "num_links": len(links),
        }
        return record, links

