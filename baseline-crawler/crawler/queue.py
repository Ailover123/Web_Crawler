# Responsibilities:
# - maintain crawl order (BFS)
# - prevent duplicate crawling within a single run
# - enforce maximum crawl depth
from collections import deque


class CrawlQueue:
    def __init__(self, max_depth):
        # Initialize crawl queue.
        self.queue = deque()      # FIFO queue for BFS crawling
        self.visited = set()      # URLs already crawled in this run
        self.queued = set()       # URLs currently enqueued
        self.max_depth = max_depth

    def enqueue(self, url, depth):
        # Rules:
        # - depth must not exceed max_depth
        # - URL must not have been visited already or already queued
        if depth > self.max_depth:
            return False

        # Block common binary / media / docs on queue level
        if not self.is_allowed_to_crawl(url):
            return False

        if url in self.visited or url in self.queued:
            return False

        self.queue.append((url, depth))
        self.queued.add(url)
        return True

    def dequeue(self):
        # Get the next URL to crawl. Marks URL as visited when dequeued.
        if not self.queue:
            return None

        url, depth = self.queue.popleft()
        # Move from queued -> visited
        try:
            self.queued.discard(url)
        except Exception:
            pass
        self.visited.add(url)
        return url, depth

    def is_empty(self):
        return not bool(self.queue)

    def is_queued(self, url):
        return url in self.queued

    def is_allowed_to_crawl(self, url):
        blocked_exts = ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.zip', '.rar', '.exe', '.tar', '.gz', '.mp3', '.mp4', '.avi', '.mov', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx']
        from urllib.parse import urlparse
        try:
            path = urlparse(url).path.lower()
            for e in blocked_exts:
                if path.endswith(e):
                    return False
        except Exception:
            return False
        return True
