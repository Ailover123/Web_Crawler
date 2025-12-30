"""
Worker thread for the crawler.
Each worker fetches a URL, parses it, enqueues new URLs, and marks as visited.
"""

import threading
from crawler.fetcher import fetch
from crawler.parser import extract_urls
import time

class Worker(threading.Thread):
    """
    Crawler worker thread.
    Runs in a loop: dequeue URL, fetch, parse, enqueue new URLs, mark visited.
    Exits when no more URLs and queue empty.
    """

    def __init__(self, frontier, name="Worker"):
        super().__init__(name=name)
        self.frontier = frontier
        self.running = True

    def run(self):
        """
        Main worker loop.
        """
        print(f"[WORKER-{self.name}] started")
        while self.running:
            item = self.frontier.dequeue()
            if item is None:
                # No item currently available; sleep briefly and retry
                time.sleep(0.1)
                continue

            url, discovered_from, depth = item

            print(f"[WORKER-{self.name}] dequeued:", url)

            try:
                print(f"[FETCH] {url}")
                # Fetch
                response = fetch(url, discovered_from, depth)
                if response:
                    # Parse and enqueue new URLs
                    html = response.text
                    new_urls = extract_urls(html, url)
                    for new_url in new_urls:
                        ok = self.frontier.enqueue(new_url, url, depth + 1)
                        if not ok:
                            print(f"[WORKER-{self.name}] enqueue rejected: {new_url}")
                        else:
                            print(f"[WORKER-{self.name}] enqueued: {new_url}")
                else:
                    print(f"[WORKER-{self.name}] fetch returned no response for {url}")
            except Exception as e:
                # Log the exception to prevent silent failures that cause crawler to hang
                print(f"[WORKER-{self.name}] Error processing {url}: {e}")
                import traceback
                traceback.print_exc()

            # Always mark as visited to prevent hanging, even on error
            try:
                self.frontier.mark_visited(url)
            except Exception as e:
                print(f"[WORKER-{self.name}] mark_visited failed for {url}: {e}")
                import traceback
                traceback.print_exc()

    def stop(self):
        """
        Stop the worker.
        """
        self.running = False
