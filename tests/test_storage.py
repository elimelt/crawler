import os
import tempfile

from crawlerlib.storage import SqliteStore


def test_sqlite_store_frontier_and_pages():
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "crawl.db")
        store = SqliteStore(db)
        assert store.mark_enqueued("https://a.com", 0)
        assert not store.mark_enqueued("https://a.com", 0)  # dedupe
        frontier = store.load_frontier()
        assert frontier == [("https://a.com", 0)]
        rec = {"url": "https://a.com", "status": 200, "content_type": "text/html", "title": "", "description": "", "text": "", "num_links": 0}
        store.save_page(rec, 0)
        assert store.seen_url("https://a.com")
        store.dequeue("https://a.com")
        assert store.load_frontier() == []

