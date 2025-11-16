#!/usr/bin/env python3
import argparse
import logging
from urllib.parse import urlparse

from crawlerlib.config import CrawlConfig, DEFAULT_USER_AGENT
from crawlerlib.engine import Crawler
from crawlerlib.prometheus_exporter import PrometheusExporter


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Domain-aware, robots-friendly web crawler with Prometheus metrics.")
    parser.add_argument("--start", nargs="+", required=True, help="One or more starting URLs.")
    parser.add_argument(
        "--allowed-domain",
        dest="allowed_domains",
        nargs="+",
        default=None,
        help="Domains to allow (e.g., example.com). Defaults to domains of --start.",
    )
    parser.add_argument("--max-pages", type=int, default=200, help="Maximum number of pages to crawl.")
    parser.add_argument("--max-depth", type=int, default=2, help="Maximum crawl depth from any start URL.")
    parser.add_argument("--concurrency", type=int, default=8, help="Number of concurrent workers.")
    parser.add_argument("--delay", type=float, default=0.5, help="Per-host politeness delay in seconds.")
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP read timeout in seconds.")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="User-Agent header to send.")
    parser.add_argument("--out", dest="output_path", default="crawl.jsonl", help="Path to JSONL output file.")
    parser.add_argument("--ignore-robots", action="store_true", help="Ignore robots.txt (not recommended).")
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase logging verbosity.")
    parser.add_argument("--sqlite", dest="sqlite_path", default=None, help="Path to SQLite DB for persistence.")
    parser.add_argument("--resume", action="store_true", help="Resume from SQLite frontier (requires --sqlite).")
    parser.add_argument("--metrics-interval", type=float, default=10.0, help="Seconds between perf logs (0 to disable).")
    parser.add_argument("--max-connections", type=int, default=16, help="Max connections per pool for HTTP client.")
    parser.add_argument("--prometheus-port", type=int, default=8000, help="Port for Prometheus metrics endpoint.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    log_level = logging.WARNING
    if args.verbose == 1:
        log_level = logging.INFO
    elif args.verbose >= 2:
        log_level = logging.DEBUG
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
    )
    
    if args.allowed_domains is None:
        inferred_domains = []
        for u in args.start:
            host = urlparse(u if "://" in u else "https://" + u).netloc
            if host:
                inferred_domains.append(host)
        allowed_domains = list({d.lower() for d in inferred_domains})
    else:
        allowed_domains = [d.lower() for d in args.allowed_domains]
    
    config = CrawlConfig(
        start_urls=args.start,
        allowed_domains=allowed_domains,
        max_pages=max(1, args.max_pages),
        max_depth=max(0, args.max_depth),
        concurrency=max(1, args.concurrency),
        max_connections=max(1, args.max_connections),
        delay_seconds=max(0.0, args.delay),
        request_timeout=max(1.0, args.timeout),
        user_agent=args.user_agent,
        obey_robots_txt=not args.ignore_robots,
        output_path=args.output_path,
        sqlite_path=args.sqlite_path,
        resume=bool(args.sqlite_path and args.resume),
        metrics_interval=max(0.0, args.metrics_interval),
    )
    
    crawler = Crawler(config)
    
    # Start Prometheus metrics exporter
    exporter = PrometheusExporter(crawler.metrics, port=args.prometheus_port)
    exporter.start()
    logging.info(f"Prometheus metrics available at http://0.0.0.0:{args.prometheus_port}/metrics")
    
    try:
        crawler.run()
    finally:
        exporter.stop()


if __name__ == "__main__":
    main()

