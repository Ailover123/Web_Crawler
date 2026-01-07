"""Replace entire worker.py with deadlock-safe version"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import threading
import time
import json
from queue import Queue, Empty
from urllib.parse import urlparse
from datetime import datetime
from zoneinfo import ZoneInfo
import psutil
import os
import tracemalloc

from crawler.fetcher import fetch
from crawler.parser import extract_urls
from crawler.storage.db import get_connection
from crawler.metrics import get_metrics
from crawler.normalizer import normalize_url


# Batching and retry settings for DB writes
DB_WRITE_QUEUE = Queue(maxsize=2000)
BATCH_SIZE = 50
FLUSH_INTERVAL = 1.0  # seconds
MAX_RETRIES = 2  # retries beyond the first attempt


class DBWriter(threading.Thread):
    """Background writer to batch DB inserts and cut contention."""

    def __init__(self):
        super().__init__(name="DBWriter", daemon=True)
        self.buffer = []
        self.last_flush = time.time()
        self.running = True

    def run(self):
        while self.running:
            try:
                task = DB_WRITE_QUEUE.get(timeout=0.5)
                if task is None:  # sentinel
                    self._flush()
                    continue
                self.buffer.append(task)
            except Empty:
                pass

            now = time.time()
            if len(self.buffer) >= BATCH_SIZE or (self.buffer and now - self.last_flush >= FLUSH_INTERVAL):
                self._flush()

        # Final flush when stopping
        self._flush()

    def stop(self):
        self.running = False
        DB_WRITE_QUEUE.put(None)

    def _flush(self):
        if not self.buffer:
            return

        batch = self.buffer
        self.buffer = []
        self.last_flush = time.time()

        attempt = 0
        while attempt <= MAX_RETRIES:
            conn = None
            try:
                conn = get_connection()
                cursor = conn.cursor()
                for task in batch:
                    self._ensure_site_record(cursor, task["url_key"], task["app_type"], task["custid"])
                    cursor.execute(
                        """
                        INSERT INTO crawl_metrics (url, fetch_status, speed_ms, size_bytes, time)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (task["url_key"], task["fetch_status"], task["speed_ms"], task["size_bytes"], task["timestamp"]),
                    )
                conn.commit()
                return
            except Exception as e:
                if conn:
                    conn.rollback()
                attempt += 1
                if attempt > MAX_RETRIES:
                    print(f"[DBWriter] Failed to flush batch after {attempt} attempts: {e}")
                    try:
                        get_metrics().record_db_batch_failure(str(e), attempt, len(batch))
                    except Exception:
                        pass
                    return
                backoff = 0.1 * (2 ** (attempt - 1))
                time.sleep(backoff)
            finally:
                if conn:
                    conn.close()

    def _ensure_site_record(self, cursor, url_key: str, app_type: str, custid: int) -> int:
        """Insert or update the sites table and return siteid for this URL (deadlock-safe)."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                cursor.execute(
                    "SELECT MAX(siteid) FROM sites WHERE custid = %s",
                    (custid,)
                )
                result = cursor.fetchone()
                max_siteid = result[0] if result and result[0] else custid
                new_siteid = max_siteid + 1

                cursor.execute(
                    """
                    INSERT IGNORE INTO sites (url, app_type, siteid, custid, added_by, time)
                    VALUES (%s, %s, %s, %s, NULL, NOW())
                    """,
                    (url_key, app_type, new_siteid, custid),
                )

                cursor.execute("SELECT siteid FROM sites WHERE url = %s", (url_key,))
                row = cursor.fetchone()
                return row[0] if row else None
            except Exception as e:
                if "1213" in str(e) and attempt < max_retries - 1:  # Deadlock error code
                    wait_time = 0.1 * (2 ** attempt)
                    time.sleep(wait_time)
                    continue
                raise


_db_writer = DBWriter()
_db_writer_started = False


def ensure_db_writer_started():
    global _db_writer_started
    if not _db_writer_started:
        _db_writer.start()
        _db_writer_started = True


class Worker(threading.Thread):
    """
    Crawler worker thread: dequeue URL, fetch, parse, enqueue, and mark visited.
    """

    def __init__(self, frontier, name="Worker", is_retry_worker=False):
        super().__init__(name=name)
        self.frontier = frontier
        self.running = True
        self.is_retry_worker = is_retry_worker
        self.sr_no = 0
        self.crawl_data = []
        self.cpu_percent_samples = []
        self.memory_usage_samples = []
        self.process = psutil.Process(os.getpid())
        tracemalloc.start()
        ensure_db_writer_started()

    def run(self):
        print(f"[WORKER-{self.name}] started")
        while self.running:
            item = self.frontier.dequeue()
            if item is None:
                time.sleep(0.1)
                continue

            url, discovered_from, depth = item

            print(f"[WORKER-{self.name}] dequeued: {url}")

            cpu_start = self.process.cpu_percent(interval=None)
            mem_start = self.process.memory_info().rss / 1024 / 1024

            try:
                fetch_start = time.time()
                fetch_result = fetch(url, discovered_from, depth)
                fetch_time = time.time() - fetch_start
                domain = urlparse(url).netloc
                timestamp = datetime.now(ZoneInfo("Asia/Kolkata"))
                custid = int(os.environ.get("DEFAULT_CUST_ID", 100))
                app_type = "website"
                url_key = self._strip_scheme(normalize_url(url))
                url_key = url_key[:100]  # Cap at 100 chars

                if fetch_result["success"]:
                    response = fetch_result["response"]
                    parse_start = time.time()
                    html = response.text
                    new_urls, assets = extract_urls(html, url)
                    parse_time = time.time() - parse_start

                    mem_end = self.process.memory_info().rss / 1024 / 1024
                    memory = mem_end
                    size = len(response.content)
                    crawl_time = fetch_time + parse_time
                    self.sr_no += 1

                    cpu_end = self.process.cpu_percent(interval=0.1)
                    self.cpu_percent_samples.append(cpu_end)
                    self.memory_usage_samples.append(mem_end - mem_start)

                    metrics = get_metrics()
                    metrics.record_url(url, domain, "success", size, crawl_time, memory, self.name, is_retry=self.is_retry_worker)
                    metrics.print_url_row(url, domain, "success", size, crawl_time, memory, self.name)

                    for new_url in new_urls:
                        self.frontier.enqueue(new_url, url, depth + 1)

                    if assets:
                        self.frontier.record_assets(url, assets)

                    DB_WRITE_QUEUE.put(
                        {
                            "url_key": url_key,
                            "fetch_status": response.status_code,
                            "speed_ms": crawl_time * 1000.0,
                            "size_bytes": size,
                            "timestamp": timestamp,
                            "app_type": app_type,
                            "custid": custid,
                        }
                    )
                else:
                    memory = 0
                    size = 0
                    self.sr_no += 1
                    error_reason = fetch_result["error"]
                    fetch_status = fetch_result.get("status", "failed")

                    cpu_end = self.process.cpu_percent(interval=0.1)
                    mem_end = self.process.memory_info().rss / 1024 / 1024
                    self.cpu_percent_samples.append(cpu_end)
                    self.memory_usage_samples.append(max(0, mem_end - mem_start))

                    metrics = get_metrics()
                    metrics.record_url(url, domain, fetch_status, size, fetch_time, memory, self.name, error_reason, is_retry=self.is_retry_worker)
                    metrics.print_url_row(url, domain, fetch_status, size, fetch_time, memory, self.name, error_reason)

                    DB_WRITE_QUEUE.put(
                        {
                            "url_key": url_key,
                            "fetch_status": 0,
                            "speed_ms": fetch_time * 1000.0,
                            "size_bytes": 0,
                            "timestamp": timestamp,
                            "app_type": app_type,
                            "custid": custid,
                        }
                    )
            except Exception as e:
                print(f"[WORKER-{self.name}] Error processing {url}: {e}")
                import traceback
                traceback.print_exc()

            try:
                self.frontier.mark_visited(url)
            except Exception as e:
                print(f"[WORKER-{self.name}] mark_visited failed for {url}: {e}")
                import traceback
                traceback.print_exc()

    def stop(self):
        self.running = False

    def _strip_scheme(self, url: str) -> str:
        """Strip scheme and trailing slashes from URL."""
        if "://" in url:
            url = url.split("://", 1)[1]
        # Remove trailing slash except for root (domain only)
        if url.endswith('/') and '/' in url.split('/', 1)[1:]:  # Has path beyond domain
            url = url.rstrip('/')
        return url

    def _ensure_site_record(self, cursor, url_key: str, app_type: str, custid: int) -> int:
        """Insert or update the sites table and return siteid for this URL.
        Handles deadlocks by retrying with exponential backoff.
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Get max siteid for this customer
                cursor.execute(
                    "SELECT MAX(siteid) FROM sites WHERE custid = %s",
                    (custid,)
                )
                result = cursor.fetchone()
                max_siteid = result[0] if result and result[0] else custid
                new_siteid = max_siteid + 1
                
                # Insert with calculated siteid; ignore if URL already exists
                cursor.execute(
                    """
                    INSERT IGNORE INTO sites (url, app_type, siteid, custid, added_by, time)
                    VALUES (%s, %s, %s, %s, NULL, NOW())
                    """,
                    (url_key, app_type, new_siteid, custid),
                )
                
                # Get the actual siteid for this URL
                cursor.execute("SELECT siteid FROM sites WHERE url = %s", (url_key,))
                row = cursor.fetchone()
                return row[0] if row else None
            except Exception as e:
                if "1213" in str(e) and attempt < max_retries - 1:  # Deadlock error code
                    wait_time = 0.1 * (2 ** attempt)  # Exponential backoff
                    time.sleep(wait_time)
                    continue
                raise
