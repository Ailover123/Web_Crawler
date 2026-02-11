from pathlib import Path

# Configuration for the web crawler.
# This file defines crawl scope, limits, and seed URLs.
# No depth logic, normalization, or defacement detection here.



# Domains allowed to crawl
# Empty = restrict to seed domains only
ALLOWED_DOMAINS = []

# Network timeout for HTTP requests (seconds)
REQUEST_TIMEOUT = 10

# User-Agent string for crawler identification
# Use a modern browser UA to reduce bot challenges
USER_AGENT = "Chrome/126.0.0.0"
# Canonical data directory for the crawler. Set to the `data` folder
# located inside the `baseline-crawler` package.
try:
    DATA_DIR = Path(__file__).resolve().parents[1] / 'data'
except NameError:
    # Fallback to os.path (string) if Path is not defined for any reason
    import os
    DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

import os
from dotenv import load_dotenv

# Load .env at the beginning of config
load_dotenv(Path(__file__).resolve().parents[2] / '.env')

# Worker scaling parameters
MIN_WORKERS = int(os.getenv("MIN_WORKERS", 5))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 10))
MAX_PARALLEL_SITES = int(os.getenv("MAX_PARALLEL_SITES", 3))
CRAWL_DELAY = 1.0  # Seconds between requests per worker to avoid 429s

# Playwright / JS Rendering Waiting Periods (seconds)
JS_GOTO_TIMEOUT = 30
JS_WAIT_TIMEOUT = 8
JS_STABILITY_TIME = 5
