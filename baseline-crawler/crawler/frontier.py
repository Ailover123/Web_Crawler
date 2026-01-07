"""
Thread-safe frontier for the web crawler.
Manages the queue of URLs to crawl, visited URLs, and in-progress URLs.
Ensures no URL is crawled twice.
Filters URLs to only enqueue meaningful HTML pages, blocking assets, CMS internals, etc.
"""

from queue import Queue
from threading import Lock
from urllib.parse import urlparse
import logging
import psutil
import os
from crawler.normalizer import normalize_url

logger = logging.getLogger(__name__)

def should_enqueue(url: str) -> bool:
    """
    Centralized URL policy function to filter URLs before enqueuing.
    Rejects URLs that are not usable for crawling meaningful site pages.
    """
    parsed = urlparse(url)

    # Reject non-HTTP schemes
    if parsed.scheme in ['mailto', 'tel', 'javascript']:
        logger.info(f"Rejected URL: {url}, reason: non-HTTP scheme ({parsed.scheme})")
        return False

    # Reject URLs with fragments (#)
    if parsed.fragment:
        logger.info(f"Rejected URL: {url}, reason: contains fragment ({parsed.fragment})")
        return False

    path = parsed.path.lower()

    # Allow asset extensions (uncommented and allowed)
    allowed_exts = {'.css', '.js', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.woff', '.woff2', '.ttf', '.eot', '.pdf', '.zip', '.rar', '.tar', '.gz', '.mp3', '.mp4', '.avi', '.mov'}
    if any(path.endswith(ext) for ext in allowed_exts):
        logger.info(f"Allowed URL: {url}, reason: asset extension")
        # Allow assets to be enqueued

    url_lower = url.lower()

    # Reject URLs containing blocked substrings
    blocked_substrings = ['canvas', 'elementor']
    if any(substring in url_lower for substring in blocked_substrings):
        logger.info(f"Rejected URL: {url}, reason: contains blocked substring")
        return False

    return True  # Allow if no rejections match

class Frontier:
    """
    Thread-safe frontier using a queue, visited set, and in-progress set.
    Prevents duplicate crawling by checking visited before enqueuing.
    Stores (url, discovered_from, depth) tuples.
    """

    def __init__(self):
        self.queue = Queue(maxsize=10_000)  # Thread-safe queue for (url, discovered_from, depth)

        # Core crawl state (visited / in_progress / discovered) guarded by a single lightweight lock
        self.state_lock = Lock()
        self.visited = set()       # URLs that have been crawled
        self.in_progress = set()   # URLs currently being fetched
        self.discovered = set()    # All URLs discovered

        # Asset tracking uses its own lock to avoid blocking enqueue/dequeue
        self.assets_lock = Lock()
        self.assets = {}  # Assets discovered: {source_url: set(unique_asset_urls)} - DEDUPED with set!

    def enqueue(self, url, discovered_from=None, depth=0):
        """
        Enqueue a URL if not visited, not in progress, and allowed by policy.
        Reserves the URL immediately under the lock.
        Filtering happens here to prevent workers from seeing blocked URLs.
        Returns True if enqueued, False otherwise.
        """
        # Quick policy check without locks
        if not should_enqueue(url):
            logger.info(f"enqueue: blocked by policy: {url}")
            return False

        normalized_url = normalize_url(url)

        # Reserve slot in in_progress under state lock
        with self.state_lock:
            if normalized_url in self.visited:
                logger.info(f"enqueue: skipped (already visited): {normalized_url}")
                return False
            if normalized_url in self.in_progress:
                logger.info(f"enqueue: skipped (already in progress): {normalized_url}")
                return False

            # Reserve immediately to avoid double work
            self.in_progress.add(normalized_url)
            try:
                self.queue.put((url, discovered_from, depth), block=False)
            except Exception as e:
                logger.exception(f"enqueue: queue.put failed for {url}: {e}")
                self.in_progress.discard(normalized_url)
                return False

            self.discovered.add(normalized_url)

        logger.info(
            f"enqueue: successfully queued {normalized_url} (depth={depth}, discovered_from={discovered_from}) "
            f"qsize={self.queue.qsize()} in_progress={len(self.in_progress)} visited={len(self.visited)}"
        )
        return True

    def dequeue(self):
        """
        Dequeue a URL tuple (url, discovered_from, depth).
        Returns the tuple or None if queue empty.
        """
        try:
            item = self.queue.get(block=False)
            logger.debug(f"dequeue: returning item {item} qsize={self.queue.qsize()} in_progress={len(self.in_progress)} visited={len(self.visited)}")
            return item
        except Exception:
            return None
    def record_assets(self, source_url, asset_urls):
        """
        Record unique assets found on a page (PDFs, images, docs only).
        Uses set to automatically deduplicate assets across pages.
        """
        normalized_source = normalize_url(source_url)
        if asset_urls:
            ASSET_EXTENSIONS = {'.pdf', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp',
                               '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx'}
            filtered_assets = [url for url in asset_urls 
                              if any(url.lower().endswith(ext) for ext in ASSET_EXTENSIONS)]

            if filtered_assets:
                with self.assets_lock:
                    if normalized_source not in self.assets:
                        self.assets[normalized_source] = set()  # Deduplicate automatically
                    self.assets[normalized_source].update(filtered_assets)
                logger.info(f"record_assets: stored {len(filtered_assets)} assets from {normalized_source}")
    def mark_visited(self, url):
        """
        Mark a URL as visited and remove from in-progress.
        This is the only release point.
        """
        from crawler.normalizer import normalize_url
        normalized = normalize_url(url)
        with self.state_lock:
            if normalized in self.in_progress:
                self.in_progress.discard(normalized)
            else:
                logger.debug(f"mark_visited: normalized url not in in_progress: {normalized}")
            self.visited.add(normalized)
        try:
            self.queue.task_done()
        except Exception:
            # Guard against task_done mismatches
            logger.debug("queue.task_done() raised an exception in mark_visited")

    def is_empty(self):
        """
        Check if queue is empty and no URLs in progress.
        """
        with self.state_lock:
            in_progress_count = len(self.in_progress)
        empty = self.queue.empty() and in_progress_count == 0
        logger.debug(f"is_empty: queue_empty={self.queue.empty()} in_progress={len(self.in_progress)} -> {empty}")
        return empty

    def get_stats(self):
        """
        Return stats: queue size, visited count, in-progress count.
        """
        with self.state_lock:
            visited_count = len(self.visited)
            in_progress_count = len(self.in_progress)
        return {
            'queue_size': self.queue.qsize(),
            'visited_count': visited_count,
            'in_progress_count': in_progress_count
        }

    def get_memory_stats(self):
        """
        Return memory stats for frontier structures.
        """
        process = psutil.Process(os.getpid())
        total_memory = process.memory_info().rss / 1024 / 1024  # MB
        # Approximate memory per structure (rough estimates)
        queue_memory = self.queue.qsize() * 0.1  # ~100 bytes per item
        visited_memory = len(self.visited) * 0.05  # ~50 bytes per URL string
        in_progress_memory = len(self.in_progress) * 0.05
        discovered_memory = len(self.discovered) * 0.05
        # Count unique assets across all pages
        total_unique_assets = len(set().union(*self.assets.values())) if self.assets else 0
        assets_memory = total_unique_assets * 0.05  # ~50 bytes per unique asset URL
        routing_graph_memory = 0  # Removed from memory to save space
        frontier_memory = queue_memory + visited_memory + in_progress_memory + discovered_memory + assets_memory
        return {
            'total_process_memory_mb': total_memory,
            'frontier_memory_mb': frontier_memory,
            'queue_memory_mb': queue_memory,
            'visited_memory_mb': visited_memory,
            'in_progress_memory_mb': in_progress_memory,
            'discovered_memory_mb': discovered_memory,
            'assets_memory_mb': assets_memory,
            'total_unique_assets': total_unique_assets
        }
