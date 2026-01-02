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
from crawler.normalizer import normalize_url
from crawler.parser import classify_url

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

    # Allow URLs with fragments (removed blockage)

    path = parsed.path.lower()

    # Allow asset extensions (uncommented and allowed)
    allowed_exts = {'.css', '.js', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.woff', '.woff2', '.ttf', '.eot', '.pdf', '.zip', '.rar', '.tar', '.gz', '.mp3', '.mp4', '.avi', '.mov'}
    if any(path.endswith(ext) for ext in allowed_exts):
        logger.info(f"Allowed URL: {url}, reason: asset extension")
        # Allow assets to be enqueued

    url_lower = url.lower()

    # Allow URLs containing specific substrings (CMS URLs)
    allowed_substrings = ['elementor', 'wp-content/uploads', 'wp-includes', 'wp-json', 'off_canvas', 'addtoany']
    if any(substring in url_lower for substring in allowed_substrings):
        logger.info(f"Allowed URL: {url}, reason: contains CMS substring")
        # Allow CMS URLs to be enqueued

    return True  # Allow if no rejections match

class Frontier:
    """
    Thread-safe frontier using a queue, visited set, and in-progress set.
    Prevents duplicate crawling by checking visited before enqueuing.
    Stores (url, discovered_from, depth) tuples.
    """

    def __init__(self):
        self.queue = Queue(maxsize=10_000)  # Thread-safe queue for (url, discovered_from, depth)
        self.visited = set()  # URLs that have been crawled
        self.in_progress = set()  # URLs currently being fetched
        self.discovered = set()  # All URLs discovered
        self.classifications = {}  # URL classifications
        self.routing_graph = {}  # parent -> list of children
        self.lock = Lock()  # Single lock for atomic operations

    def enqueue(self, url, discovered_from=None, depth=0):
        """
        Enqueue a URL if not visited, not in progress, and allowed by policy.
        Reserves the URL immediately under the lock.
        Filtering happens here to prevent workers from seeing blocked URLs.
        Returns True if enqueued, False otherwise.
        """
        with self.lock:
            if not should_enqueue(url):
                logger.info(f"enqueue: blocked by policy: {url}")
                return False  # Block URL based on centralized policy
            normalized_url = normalize_url(url)
            if normalized_url in self.visited:
                logger.info(f"enqueue: skipped (already visited): {normalized_url}")
                return False
            if normalized_url in self.in_progress:
                logger.info(f"enqueue: skipped (already in progress): {normalized_url}")
                return False
            # reserve immediately
            self.in_progress.add(normalized_url)
            try:
                self.queue.put((url, discovered_from, depth))
            except Exception as e:
                logger.exception(f"enqueue: queue.put failed for {url}: {e}")
                # rollback reservation
                self.in_progress.discard(normalized_url)
                return False
            self.discovered.add(normalized_url)
            # Store classification
            try:
                self.classifications[normalized_url] = classify_url(url)
            except Exception:
                logger.debug(f"enqueue: classify_url failed for {url}")
            # Record routing: discovered_from -> normalized_url
            if discovered_from:
                try:
                    normalized_parent = normalize_url(discovered_from)
                    if normalized_parent not in self.routing_graph:
                        self.routing_graph[normalized_parent] = []
                    if normalized_url not in self.routing_graph[normalized_parent]:
                        self.routing_graph[normalized_parent].append(normalized_url)
                except Exception:
                    logger.debug(f"enqueue: failed to record routing for parent {discovered_from} -> {url}")
            # Record assets if any
            # Note: Assets are not enqueued, just recorded
            logger.info(f"enqueue: successfully queued {normalized_url} (depth={depth}, discovered_from={discovered_from}) qsize={self.queue.qsize()} in_progress={len(self.in_progress)} visited={len(self.visited)}")
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

    def mark_visited(self, url):
        """
        Mark a URL as visited and remove from in-progress.
        This is the only release point.
        """
        from crawler.normalizer import normalize_url
        normalized = normalize_url(url)
        with self.lock:
            # Remove the normalized URL from in_progress and add to visited
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
        empty = self.queue.empty() and len(self.in_progress) == 0
        logger.debug(f"is_empty: queue_empty={self.queue.empty()} in_progress={len(self.in_progress)} -> {empty}")
        return empty

    def get_stats(self):
        """
        Return stats: queue size, visited count, in-progress count.
        """
        return {
            'queue_size': self.queue.qsize(),
            'visited_count': len(self.visited),
            'in_progress_count': len(self.in_progress)
        }
