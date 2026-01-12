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
from datetime import datetime
import tldextract
from crawler.normalizer import normalize_url
from crawler.policy import URLPolicy

logger = logging.getLogger(__name__)

# Thread-safe file logger for scope rejections
_scope_rejection_lock = Lock()
_SCOPE_LOG_FILE = "scope_rejections.txt"

def _log_scope_rejection(siteid, site_root, rejected_url, rejected_host, reason="scope_mismatch"):
    """Thread-safe file logger for scope rejections. Appends to scope_rejections.txt."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = (
        f"[REJECT][SCOPE]\n"
        f"timestamp={timestamp}\n"
        f"siteid={siteid}\n"
        f"site_root={site_root}\n"
        f"rejected_url={rejected_url}\n"
        f"rejected_host={rejected_host}\n"
        f"reason={reason}\n\n"
    )
    with _scope_rejection_lock:
        try:
            with open(_SCOPE_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(log_entry)
        except Exception as e:
            logger.warning(f"Failed to write scope rejection log: {e}")

def should_enqueue(url: str) -> bool:
    """
    Delegate URL filtering to centralized URLPolicy.
    Logs a reason when blocked for transparency.
    """
    parsed = urlparse(url)
    if not URLPolicy.is_http(url):
        logger.info(f"Rejected URL: {url}, reason: non-HTTP scheme ({parsed.scheme})")
        return False
    if URLPolicy.has_fragment(url):
        logger.info(f"Rejected URL: {url}, reason: contains fragment ({parsed.fragment})")
        return False
    if URLPolicy.is_asset(url):
        logger.info(f"Rejected URL: {url}, reason: asset/doc/media extension")
        return False
    if URLPolicy.is_blocked_path(url):
        logger.info(f"Rejected URL: {url}, reason: blocked path pattern")
        return False
    if URLPolicy.contains_blocked_substring(url):
        logger.info(f"Rejected URL: {url}, reason: contains blocked substring")
        return False
    return True

class Frontier:
    """
    Thread-safe frontier using a queue, visited set, and in-progress set.
    Prevents duplicate crawling by checking visited before enqueuing.
    Stores (url, discovered_from, depth, siteid) tuples.
    """

    def __init__(self):
        self.queue = Queue(maxsize=10_000)  # Thread-safe queue for (url, discovered_from, depth, siteid)

        # Core crawl state (visited / in_progress / discovered) guarded by a single lightweight lock
        self.state_lock = Lock()
        self.visited = set()       # URLs that have been crawled
        self.in_progress = set()   # URLs currently being fetched
        self.discovered = set()    # All URLs discovered

        # Asset tracking uses its own lock to avoid blocking enqueue/dequeue
        self.assets_lock = Lock()
        self.assets = {}  # Assets discovered: {source_url: set(unique_asset_urls)} - DEDUPED with set!

        # Site scope tracking: maps siteid -> (root_url, registrable_domain, allowed_subdomains)
        self.site_scope_lock = Lock()
        self.site_scope_map = {}  # {siteid: {"root_url": str, "domain": str, "subdomains": set}}

    

    def set_site_scope(self, siteid, root_url):
        """
        Register the root URL for a site using tldextract.
        Extracts registrable domain (domain + suffix) and allows root + www subdomain only.
        Called once per site during crawl initialization.
        """
        root_host = urlparse(root_url).netloc.lower()
        extracted = tldextract.extract(root_url)
        registrable_domain = f"{extracted.domain}.{extracted.suffix}".lower()
        
        with self.site_scope_lock:
            self.site_scope_map[siteid] = {
                "root_url": root_url,
                "domain": registrable_domain,
                "subdomains": {"", "www"}  # Allow root and www only
            }
        logger.info(f"set_site_scope: registered siteid={siteid} domain={registrable_domain} (allows: root, www)")

    def _check_site_scope(self, url, siteid):
        """
        Check if URL belongs to the same site scope using tldextract.
        Allows root domain + www subdomain only. All other subdomains blocked.
        Returns (is_in_scope: bool, site_root_url: str or None).
        """
        if siteid is None:
            return True, None  # No scope check if siteid is None

        with self.site_scope_lock:
            if siteid not in self.site_scope_map:
                return True, None  # Site not registered yet, allow
            scope_info = self.site_scope_map[siteid]

        site_domain = scope_info["domain"]
        allowed_subs = scope_info["subdomains"]
        site_root_url = scope_info["root_url"]

        # Extract URL domain and subdomain using tldextract
        extracted = tldextract.extract(url)
        url_domain = f"{extracted.domain}.{extracted.suffix}".lower()
        url_subdomain = extracted.subdomain.lower()

        # Allow if: same registrable domain AND subdomain is in allowed set
        in_scope = (url_domain == site_domain) and (url_subdomain in allowed_subs)
        
        # Debug logging for scope check
        logger.debug(
            f"_check_site_scope: url={url} | "
            f"site_domain={site_domain} url_domain={url_domain} match={url_domain == site_domain} | "
            f"url_subdomain='{url_subdomain}' allowed={allowed_subs} in_set={url_subdomain in allowed_subs} | "
            f"in_scope={in_scope}"
        )
        
        return in_scope, site_root_url

    def get_site_host(self, siteid):
        """Return the registered root URL for a siteid, or None."""
        with self.site_scope_lock:
            scope_info = self.site_scope_map.get(siteid)
            return scope_info["root_url"] if scope_info else None

    def enqueue(self, url, discovered_from=None, depth=0, siteid=None):
        """
        Enqueue a URL if not visited, not in progress, and allowed by policy.
        Reserves the URL immediately under the lock.
        Filtering happens here to prevent workers from seeing blocked URLs.
        Returns True if enqueued, False otherwise.
        """
        # Quick policy check without locks — must pass before scope logging
        if not should_enqueue(url):
            logger.info(f"enqueue: blocked by policy: {url}")
            return False

        # Now check site scope and log only scope-based rejections
        in_scope, site_root_url = self._check_site_scope(url, siteid)
        if not in_scope and site_root_url:
            url_host = urlparse(url).netloc.lower()
            # Log to file instead of terminal
            _log_scope_rejection(siteid, site_root_url, url, url_host, "scope_mismatch")
            logger.info(
                f"enqueue: rejected by scope (siteid={siteid}, site_root={site_root_url}, rejected_url={url}, rejected_host={url_host})"
            )
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
                self.queue.put((url, discovered_from, depth, siteid), block=False)
            except Exception as e:
                logger.exception(f"enqueue: queue.put failed for {url}: {e}")
                self.in_progress.discard(normalized_url)
                return False

            self.discovered.add(normalized_url)

        logger.info(
            f"enqueue: successfully queued {normalized_url} (depth={depth}, siteid={siteid}, discovered_from={discovered_from}) "
            f"qsize={self.queue.qsize()} in_progress={len(self.in_progress)} visited={len(self.visited)}"
        )
        return True

    def dequeue(self):
        """
        Dequeue a URL tuple (url, discovered_from, depth, siteid).
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
