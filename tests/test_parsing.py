import pytest

from crawlerlib.parsing import UrlTools, Extractor


def test_normalize_start():
    urls = ["example.com", "https://foo.com/path#frag", ""]
    out = UrlTools.normalize_start(urls)
    assert out[0].startswith("https://example.com")
    assert out[1] == "https://foo.com/path"
    assert len(out) == 2


def test_normalize_link_and_allowed():
    base = "https://example.com/a/b"
    assert UrlTools.normalize_link(base, "mailto:x") is None
    assert UrlTools.normalize_link(base, "javascript:void(0)") is None
    assert UrlTools.normalize_link(base, "#frag") is None
    assert UrlTools.normalize_link(base, "/c") == "https://example.com/c"
    assert UrlTools.is_allowed_domain("https://sub.example.com/x", ["example.com"])
    assert not UrlTools.is_allowed_domain("https://evil.com", ["example.com"])


def test_extract_simple():
    html = """
    <html><head>
      <title>Hi</title>
      <meta name="description" content="Desc">
    </head>
    <body>
      <a href="/x">x</a>
      <p>hello world</p>
    </body></html>
    """
    rec, links = Extractor.extract("https://example.com", html)
    assert rec["title"] == "Hi"
    assert rec["description"] == "Desc"
    assert rec["num_links"] == 1
    assert links == ["https://example.com/x"]

