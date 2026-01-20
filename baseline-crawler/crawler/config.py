from pathlib import Path

# Configuration for the web crawler.
# This file defines crawl scope, limits, and seed URLs.
# No depth logic, normalization, or defacement detection here.


# Maximum crawl depth
# 0 = only seed URLs
DEPTH_LIMIT = 2
# Maximum number of pages to crawl per run
MAX_PAGES = 100


# Domains allowed to crawl
# Empty = restrict to seed domains only
ALLOWED_DOMAINS = []

# Network timeout for HTTP requests (seconds)
REQUEST_TIMEOUT = 10

# User-Agent string for crawler identification
# Use a modern browser UA to reduce bot challenges
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)

# Canonical data directory for the crawler. Set to the `data` folder
# located inside the `baseline-crawler` package.
try:
    DATA_DIR = Path(__file__).resolve().parents[1] / 'data'
except NameError:
    # Fallback to os.path (string) if Path is not defined for any reason
    import os
    DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

# Worker scaling parameters
MIN_WORKERS = 5
MAX_WORKERS = 50
