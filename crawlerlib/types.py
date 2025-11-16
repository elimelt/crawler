from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass(frozen=True)
class FetchResult:
    status: int
    content_type: str
    text: str
    size_bytes: int


class HttpClientProtocol(Protocol):
    def fetch(self, url: str) -> Optional[FetchResult]: ...

