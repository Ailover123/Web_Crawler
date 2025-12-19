#For Config Values
#Seperate file for better change management to handle the behaviour of crawler without changing the main codebase
#Easier to test and maintain

# Initial URLs to start crawling from (MANDATORY)
SEED_URLS = [
    "http://localhost:8000/test.html"
]

# Maximum crawl depth
# 0 = only seed URLs
DEPTH_LIMIT = 0

# Domains allowed to crawl
# Empty = restrict to seed domains only
ALLOWED_DOMAINS = []

# Network timeout for HTTP requests (seconds)
REQUEST_TIMEOUT = 10

# Delay between requests to the same domain (seconds)
REQUEST_DELAY = 1

# User-Agent string for crawler identification
USER_AGENT = "BaselineCrawler/1.0"
