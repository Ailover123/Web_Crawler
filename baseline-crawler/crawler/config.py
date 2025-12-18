#For Config Values
#Seperate file for better change management to handle the behaviour of crawler without changing the main codebase
#Easier to test and maintain

#This is where crawling starts from
SEED_URLS = []

#Maximum depth to crawl
DEPTH_LIMIT = 0

#Domains allowed to crawl, only to avoid external links
ALLOWED_DOMAINS = []

#Time to wait between requests to the same domain (in seconds)
REQUEST_TIMEOUT = 5

#User-Agent string to identify the crawler
USER_AGENT = "BaselineCrawler/1.0"