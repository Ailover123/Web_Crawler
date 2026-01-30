"""
Thread-safe frontier for the web crawler.

FINAL FIX:
- Guarantees queue.task_done() is called ONLY after a successful queue.get()
- Prevents queue counter corruption
- Makes queue.join() reliable
"""

from queue import Queue, Empty
from threading import Lock
from urllib.parse import urlparse
import logging

from crawler.normalizer import normalize_url
from crawler.parser import classify_url

logger = logging.getLogger(__name__)


def should_enqueue(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme in ("mailto", "tel", "javascript"):
        return False
    return True


class Frontier:
    def __init__(self):
        self.queue = Queue(maxsize=10_000)
        self.visited = set()
        self.in_progress = set()
        self.discovered = set()
        self.classifications = {}
        self.routing_graph = {}
        self.lock = Lock()

    # ---------------- ENQUEUE ----------------
    def enqueue(self, url, discovered_from=None, depth=0) -> bool:
        with self.lock:
            if not should_enqueue(url):
                return False

            normalized = normalize_url(url)

            if normalized in self.visited or normalized in self.in_progress:
                return False

            self.in_progress.add(normalized)
            try:
                self.queue.put((normalized, discovered_from, depth))
            except Exception:
                self.in_progress.discard(normalized)
                return False

            self.discovered.add(normalized)

            try:
                self.classifications[normalized] = classify_url(url)
            except Exception:
                pass

            if discovered_from:
                try:
                    parent = normalize_url(discovered_from)
                    self.routing_graph.setdefault(parent, []).append(normalized)
                except Exception:
                    pass

            return True

    # ---------------- DEQUEUE ----------------
    def dequeue(self):
        try:
            item = self.queue.get(timeout=0.5)
            return item, True
        except Empty:
            return None, False

    # ---------------- MARK VISITED ----------------
    def mark_visited(self, url, *, got_task: bool):
        normalized = normalize_url(url)

        with self.lock:
            self.in_progress.discard(normalized)
            self.visited.add(normalized)

        if got_task:
            self.queue.task_done()

    # ---------------- STATS ----------------
    def get_stats(self):
        return {
            "queue_size": self.queue.qsize(),
            "visited_count": len(self.visited),
            "in_progress_count": len(self.in_progress),
        }
 