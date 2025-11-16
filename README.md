## crawler

Crawls the whole web (in theory)

### Setup

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### Usage

Basic crawl:

```bash
python extract.py --start https://example.com --max-pages 50 --max-depth 2
```

Specify allowed domains and output path:

```bash
python extract.py \
  --start https://example.com https://www.example.com/blog \
  --allowed-domain example.com \
  --max-pages 200 \
  --max-depth 2 \
  --concurrency 8 \
  --delay 0.5 \
  --out data/crawl.jsonl -v
```

Persist to SQLite and resume later:

```bash
python extract.py \
  --start https://example.com \
  --allowed-domain example.com \
  --sqlite data/crawl.db \
  --max-pages 1000 \
  --concurrency 12 \
  -v

# Later, resume remaining work from the stored frontier
python extract.py --sqlite data/crawl.db --resume -v
```

Ignore robots.txt (not recommended):

```bash
python extract.py --start https://example.com --ignore-robots
```

Increase verbosity:
- `-v` for info
- `-vv` for debug

### Output format
Each crawled page is written as a single JSON object on its own line (`.jsonl`):

```json
{"url":"https://example.com/","status":200,"content_type":"text/html; charset=utf-8","title":"Example Domain","description":"...","text":"... truncated text ...","num_links":23}
```

Fields:
- `url`: final URL requested
- `status`: HTTP status code
- `content_type`: Content-Type header
- `title`: document title
- `description`: meta description or OpenGraph description if present
- `text`: plain text content (first 4000 chars)
- `num_links`: number of outgoing links extracted
