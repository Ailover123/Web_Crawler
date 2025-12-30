"""
Worker thread for the crawler.
Each worker fetches a URL, parses it, enqueues new URLs, and marks as visited.
"""

import threading
from crawler.fetcher import fetch
from crawler.parser import extract_urls
from crawler.normalizer import normalize_url
from crawler.storage.db import get_connection
import time
import json
from urllib.parse import urlparse
from datetime import datetime, timezone

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
                domain = urlparse(url).netloc
                timestamp = datetime.now(timezone.utc).isoformat()
                if response:
                    # Parse and enqueue new URLs and record assets
                    html = response.text
                    new_urls, assets = extract_urls(html, url)
                    for new_url in new_urls:
                        ok = self.frontier.enqueue(new_url, url, depth + 1)
                        if not ok:
                            print(f"[WORKER-{self.name}] enqueue rejected: {new_url}")
                        else:
                            print(f"[WORKER-{self.name}] enqueued: {new_url}")
                    # Record assets in routing graph
                    normalized_parent = normalize_url(url)
                    if normalized_parent not in self.frontier.routing_graph:
                        self.frontier.routing_graph[normalized_parent] = []
                    for asset in assets:
                        normalized_asset = normalize_url(asset)
                        if normalized_asset not in self.frontier.routing_graph[normalized_parent]:
                            self.frontier.routing_graph[normalized_parent].append(normalized_asset)

                    # Record to domain-specific DB
                    conn = get_connection(domain)
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT OR REPLACE INTO crawl_data (domain, url, routed_from, urls_present_on_page, fetch_status, speed, size, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (domain, url, discovered_from, json.dumps(new_urls), response.status_code, 0.0, len(response.content), timestamp))
                    conn.commit()
                    conn.close()
                else:
                    # Record failed fetch
                    conn = get_connection(domain)
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT OR REPLACE INTO crawl_data (domain, url, routed_from, urls_present_on_page, fetch_status, speed, size, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (domain, url, discovered_from, json.dumps([]), 0, 0.0, 0, timestamp))
                    conn.commit()
                    conn.close()
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
