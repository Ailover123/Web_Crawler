# Responsibilities:
# - maintain crawl order (BFS)
# - prevent duplicate crawling within a single run
# - enforce maximum crawl depth
from collections import deque

class CrawlQueue:
    def __init__(self, max_depth):
        # """
        # Initialize crawl queue.
        self.queue = deque()      # FIFO queue for BFS crawling
        self.visited = set()      # URLs already crawled in this run
        self.max_depth = max_depth

    def enqueue(self, url, depth):
        # Rules:
        # - depth must not exceed max_depth
        # - URL must not have been visited already

        if depth > self.max_depth:
            return

        if url in self.visited:
            return

        # Do NOT mark visited here
        self.queue.append((url, depth))

    def dequeue(self):
        # Get the next URL to crawl.

        # Marks URL as visited only when dequeued,
        # ensuring retries are possible if enqueue happened but crawl failed.

        if not self.queue:
            return None

        url, depth = self.queue.popleft()
        self.visited.add(url)
        return url, depth

    def is_empty(self):
        return not self.queue
