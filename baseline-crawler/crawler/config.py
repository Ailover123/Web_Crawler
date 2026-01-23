from pathlib import Path

# Configuration for the web crawler.
# This file defines crawl scope, limits, and seed URLs.
# No depth logic, normalization, or defacement detection here.



# Domains allowed to crawl
# Empty = restrict to seed domains only
ALLOWED_DOMAINS = []

# Network timeout for HTTP requests (seconds)
REQUEST_TIMEOUT = 20

# User-Agent string for crawler identification
# Use a modern browser UA to reduce bot challenges
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
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

# Playwright / JS Rendering Waiting Periods (seconds)
JS_GOTO_TIMEOUT = 30
JS_WAIT_TIMEOUT = 8
JS_STABILITY_TIME = 5
