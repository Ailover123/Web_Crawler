"""Replace entire worker.py with deadlock-safe version"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import threading
import time
import json
from queue import Queue, Empty
from collections import defaultdict
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
from crawler.normalizer import normalize_url, extract_relative_path


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
                    # Write to crawled_urls output table (NOT to sites)
                    cursor.execute(
                        """
                        INSERT INTO crawled_urls (siteid, url, http_status, crawl_depth, crawled_at)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (task["siteid"], task["relative_path"], task["fetch_status"], task["crawl_depth"], task["timestamp"]),
                    )
                    # TEMP DISABLED: crawl_metrics not used for MVP
                    # cursor.execute(
                    #     """
                    #     INSERT INTO crawl_metrics (url, fetch_status, speed_ms, size_bytes, time)
                    #     VALUES (%s, %s, %s, %s, %s)
                    #     """,
                    #     (task["relative_path"], task["fetch_status"], task["speed_ms"], task["size_bytes"], task["timestamp"]),
                    # )
                conn.commit()
                return
            except Exception as e:
                if conn:
                    conn.rollback()
                attempt += 1
                if attempt > MAX_RETRIES:
                    print(f"[DBWriter] Failed to flush batch after {attempt} attempts: {e}")
                    try:
                        # TEMP DISABLED: record_db_batch_failure not used for MVP
                        # get_metrics().record_db_batch_failure(str(e), attempt, len(batch))
                        pass
                    except Exception:
                        pass
                    return
                backoff = 0.1 * (2 ** (attempt - 1))
                time.sleep(backoff)
            finally:
                if conn:
                    conn.close()


_db_writer = DBWriter()
_db_writer_started = False


def ensure_db_writer_started():
    global _db_writer_started
    if not _db_writer_started:
        _db_writer.start()
        _db_writer_started = True


_site_label_lock = threading.Lock()
_site_child_sequence = defaultdict(int)

_site_storage_lock = threading.Lock()
_site_storage_state = {}


def _format_site_label(siteid, depth):
    if siteid is None:
        return "[siteid=unknown]"
    with _site_label_lock:
        if depth <= 0:
            _site_child_sequence[siteid] = 0
            return f"[siteid={siteid}]"
        _site_child_sequence[siteid] += 1
        return f"[siteid={siteid}-{_site_child_sequence[siteid]:02d}]"


def _reset_site_label(siteid):
    if siteid is None:
        return
    with _site_label_lock:
        _site_child_sequence.pop(siteid, None)


def _queue_or_buffer_record(record):
    siteid = record.get("siteid")
    depth = record.get("crawl_depth", 0)
    if siteid is None:
        DB_WRITE_QUEUE.put(record)
        return

    enqueue_now = False
    with _site_storage_lock:
        state = _site_storage_state.setdefault(siteid, {"child_seen": False, "pending_root": None})
        if depth == 0:
            # Skip root URLs (those that are just "/") - don't store parent-only URLs
            relative_path = record.get("relative_path", "")
            if relative_path == "/" or relative_path == "":
                # Root URL - skip it completely, don't store or buffer
                return
            state["pending_root"] = record
        else:
            state["child_seen"] = True
            state["pending_root"] = None
            enqueue_now = True

    if enqueue_now:
        DB_WRITE_QUEUE.put(record)


def finalize_site_outputs(siteid):
    flushed_root = False
    if siteid is None:
        return flushed_root

    with _site_storage_lock:
        state = _site_storage_state.pop(siteid, None)

    if state:
        pending_root = state.get("pending_root")
        child_seen = state.get("child_seen", False)
        if pending_root and not child_seen:
            DB_WRITE_QUEUE.put(pending_root)
            flushed_root = True

    _reset_site_label(siteid)
    return flushed_root


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

            url, discovered_from, depth, siteid = item
            site_label = _format_site_label(siteid, depth)
            print(f"[WORKER-{self.name}] dequeued {site_label} {url}")

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
                relative_path = self._relative_path(url)

                if fetch_result["success"]:
                    response = fetch_result["response"]
                    parse_start = time.time()
                    html = response.text
                    # Provide site scope context to parser for diagnostic logging
                    site_root_url = self.frontier.get_site_host(siteid)
                    # Use final URL after redirects (response.url) for parser base_url
                    # This prevents scope mismatch when site redirects www <-> non-www
                    final_url = response.url
                    relative_path = self._relative_path(final_url)
                    new_urls, assets = extract_urls(html, final_url, siteid=siteid, site_root_url=site_root_url)
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
                        self.frontier.enqueue(new_url, url, depth + 1, siteid)

                    if assets:
                        self.frontier.record_assets(url, assets)

                    _queue_or_buffer_record(
                        {
                            "relative_path": relative_path,
                            "fetch_status": response.status_code,
                            "crawl_depth": depth,
                            "timestamp": timestamp,
                            "siteid": siteid,
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

                    _queue_or_buffer_record(
                        {
                            "relative_path": relative_path,
                            "fetch_status": 0,
                            "crawl_depth": depth,
                            "timestamp": timestamp,
                            "siteid": siteid,
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

    def _relative_path(self, url: str) -> str:
        """Return normalized relative path for DB storage."""
        normalized = normalize_url(url)
        return extract_relative_path(normalized)
