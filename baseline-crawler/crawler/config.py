from pathlib import Path

#For Config Values
#Seperate file for better change management to handle the behaviour of crawler without changing the main codebase
#Easier to test and maintain

# Initial URLs to start crawling from (MANDATORY)
SEED_URLS = [
    "https://worldpeoplesolutions.com/"
]

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

# Delay between requests to the same domain (seconds)
REQUEST_DELAY = 1

# User-Agent string for crawler identification
USER_AGENT = "BaselineCrawler/1.0"
    
# Canonical data directory for the crawler and UI. Set to the `data` folder
# located inside the `baseline-crawler` package (keeps all runtime data together).
# Use this constant everywhere file paths are built so we don't rely on relative
# `..` lookups or multiple copies of the same data under different folders.
# Resolve canonical data directory next to this package. Use a safe fallback
# if for some reason `Path` isn't available in the import environment
# (this guards against odd import-time issues and makes startup more robust).
try:
    DATA_DIR = Path(__file__).resolve().parents[1] / 'data'
except NameError:
    # Fallback to os.path (string) if Path is not defined for any reason
    import os
    DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
