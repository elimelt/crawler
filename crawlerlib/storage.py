import json
import sqlite3
import threading
from typing import Dict, Iterable, List, Optional, Tuple
from pathlib import Path


class JsonlWriter:
    def __init__(self, output_path: str, append: bool = False) -> None:
        self.output_path = output_path
        self._lock = threading.Lock()
        out_path = Path(self.output_path)
        if out_path.parent:
            out_path.parent.mkdir(parents=True, exist_ok=True)
        if not append:
            with out_path.open("w", encoding="utf-8") as _:
                pass

    def write(self, record: Dict) -> None:
        line = json.dumps(record, ensure_ascii=False)
        with self._lock, Path(self.output_path).open("a", encoding="utf-8") as f:
            f.write(line + "\n")


class SqliteStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._conn:
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS pages ("
                " url TEXT PRIMARY KEY,"
                " status INTEGER,"
                " content_type TEXT,"
                " title TEXT,"
                " description TEXT,"
                " text TEXT,"
                " depth INTEGER,"
                " crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS frontier ("
                " url TEXT PRIMARY KEY,"
                " depth INTEGER)"
            )
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS links ("
                " from_url TEXT,"
                " to_url TEXT,"
                " UNIQUE(from_url, to_url))"
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_frontier_depth ON frontier(depth)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_links_from ON links(from_url)")

    def load_frontier(self) -> List[Tuple[str, int]]:
        with self._lock, self._conn:
            cur = self._conn.execute("SELECT url, depth FROM frontier ORDER BY depth ASC")
            return list(cur.fetchall())

    def mark_enqueued(self, url: str, depth: int) -> bool:
        with self._lock, self._conn:
            try:
                self._conn.execute("INSERT OR IGNORE INTO frontier(url, depth) VALUES (?, ?)", (url, depth))
                cur = self._conn.execute("SELECT changes()")
                return cur.fetchone()[0] > 0
            except sqlite3.Error:
                return False

    def dequeue(self, url: str) -> None:
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM frontier WHERE url = ?", (url,))

    def seen_url(self, url: str) -> bool:
        with self._lock, self._conn:
            cur = self._conn.execute("SELECT 1 FROM pages WHERE url = ? LIMIT 1", (url,))
            if cur.fetchone():
                return True
            cur = self._conn.execute("SELECT 1 FROM frontier WHERE url = ? LIMIT 1", (url,))
            return cur.fetchone() is not None

    def save_page(self, record: Dict, depth: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO pages(url, status, content_type, title, description, text, depth) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    record.get("url"),
                    record.get("status"),
                    record.get("content_type"),
                    record.get("title"),
                    record.get("description"),
                    record.get("text"),
                    depth,
                ),
            )

    def add_links(self, from_url: str, to_urls: Iterable[str]) -> None:
        rows = [(from_url, u) for u in to_urls]
        if not rows:
            return
        with self._lock, self._conn:
            self._conn.executemany(
                "INSERT OR IGNORE INTO links(from_url, to_url) VALUES (?, ?)",
                rows,
            )

