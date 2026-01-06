"""
Worker thread for the crawler.
Each worker fetches a URL, parses it, enqueues new URLs, and marks as visited.
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import threading
import time
import json
from urllib.parse import urlparse
from datetime import datetime, timezone
import psutil
import os
import tracemalloc

from crawler.fetcher import fetch
from crawler.parser import extract_urls
from crawler.storage.db import get_connection
from crawler.metrics import get_metrics
from crawler.normalizer import normalize_url


class Worker(threading.Thread):
    """
    Crawler worker thread: dequeue URL, fetch, parse, enqueue, and mark visited.
    """

    def __init__(self, frontier, name="Worker"):
        super().__init__(name=name)
        self.frontier = frontier
        self.running = True
        self.sr_no = 0
        self.crawl_data = []
        # Per-worker CPU and memory tracking
        self.cpu_percent_samples = []
        self.memory_usage_samples = []
        self.process = psutil.Process(os.getpid())
        tracemalloc.start()

    def run(self):
        print(f"[WORKER-{self.name}] started")
        while self.running:
            item = self.frontier.dequeue()
            if item is None:
                # No item currently available; sleep briefly and retry
                time.sleep(0.1)
                continue

            url, discovered_from, depth = item

            print(f"[WORKER-{self.name}] dequeued:", url)

            # Capture CPU and memory at start of processing
            cpu_start = self.process.cpu_percent(interval=None)
            mem_start = self.process.memory_info().rss / 1024 / 1024

            try:
                # Fetch with timing
                fetch_start = time.time()
                fetch_result = fetch(url, discovered_from, depth)
                fetch_time = time.time() - fetch_start
                domain = urlparse(url).netloc
                timestamp = datetime.now(timezone.utc).isoformat()
                if fetch_result['success']:
                    response = fetch_result['response']
                    # Parse with timing
                    parse_start = time.time()
                    html = response.text
                    new_urls, assets = extract_urls(html, url)
                    parse_time = time.time() - parse_start
                    # Memory measurement after processing
                    mem_end = self.process.memory_info().rss / 1024 / 1024
                    memory = mem_end
                    size = len(response.content)
                    # Calculate crawl_time as total time for fetch + parse + db
                    crawl_time = fetch_time + parse_time
                    self.sr_no += 1
                    domain = urlparse(url).netloc
                    
                    # Record CPU and memory samples
                    cpu_end = self.process.cpu_percent(interval=0.1)
                    self.cpu_percent_samples.append(cpu_end)
                    self.memory_usage_samples.append(mem_end - mem_start)
                    
                    # Record and print metrics
                    metrics = get_metrics()
                    metrics.record_url(url, domain, "success", size, crawl_time, memory, self.name)
                    metrics.print_url_row(url, domain, "success", size, crawl_time, memory, self.name)

                    for new_url in new_urls:
                        self.frontier.enqueue(new_url, url, depth + 1)
                    
                    # Record assets found on this page (PDFs, images, media in HTML tags)
                    if assets:
                        self.frontier.record_assets(url, assets)

                    db_start = time.time()
                    conn = get_connection(domain)
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO crawl_data (domain, url, routed_from, urls_present_on_page, fetch_status, speed, size, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (domain, normalize_url(url), discovered_from, json.dumps(new_urls), response.status_code, crawl_time, size, timestamp),
                    )
                    conn.commit()
                    conn.close()
                    db_time = time.time() - db_start
                    # Add db_time to crawl_time
                    crawl_time += db_time
                else:
                    memory = 0  # or measure
                    size = 0
                    self.sr_no += 1
                    domain = urlparse(url).netloc
                    error_reason = fetch_result['error']
                    fetch_status = fetch_result.get('status', 'failed')  # 'skipped', 'not_found', or 'failed'
                    
                    # Record CPU and memory samples for failures too
                    cpu_end = self.process.cpu_percent(interval=0.1)
                    mem_end = self.process.memory_info().rss / 1024 / 1024
                    self.cpu_percent_samples.append(cpu_end)
                    self.memory_usage_samples.append(max(0, mem_end - mem_start))
                    
                    # Record and print metrics
                    metrics = get_metrics()
                    metrics.record_url(url, domain, fetch_status, size, fetch_time, memory, self.name, error_reason)
                    metrics.print_url_row(url, domain, fetch_status, size, fetch_time, memory, self.name, error_reason)
                    
                    # Record failed fetch
                    db_start = time.time()
                    conn = get_connection(domain)
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO crawl_data (domain, url, routed_from, urls_present_on_page, fetch_status, speed, size, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (domain, normalize_url(url), discovered_from, json.dumps([]), 0, fetch_time, 0, timestamp),
                    )
                    conn.commit()
                    conn.close()
                    db_time = time.time() - db_start
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
        self.running = False
