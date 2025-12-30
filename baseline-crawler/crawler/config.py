from pathlib import Path

# Configuration for the web crawler.
# This file defines crawl scope, limits, and seed URLs.
# No depth logic, normalization, or defacement detection here.

# Initial URLs to start crawling from (MANDATORY)
SEED_URLS = [
    "https://worldpeoplesolutions.com/",
    "https://sitewall.net/",
    "https://uat.pagentra.com/"
]

# Network timeout for HTTP requests (seconds)
REQUEST_TIMEOUT = 10

# User-Agent string for crawler identification
USER_AGENT = "BaselineCrawler/1.0"

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
