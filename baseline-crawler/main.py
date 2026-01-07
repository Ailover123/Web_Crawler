"""
Entry point for the web crawler.
Initializes DB, seeds frontier, starts workers, waits for completion.
Computes per-domain crawl statistics, prints summaries, and dumps debug JSON.
"""

from crawler.storage.db import initialize_db, get_connection, initialize_failed_db
from crawler.config import SEED_URLS,MIN_WORKERS,MAX_WORKERS
from crawler.frontier import Frontier
from crawler.worker import Worker
from crawler.metrics import reset_metrics, get_metrics
import time
import os
import json
import datetime
from urllib.parse import urlparse
from threading import Lock
import psutil

def get_db_stats():
    """Get storage stats from centralized MySQL DB (sites table)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM sites")
    total_rows = cursor.fetchone()[0]
    
    # Calculate actual database size from crawl_metrics table
    cursor.execute("SELECT SUM(size_bytes) FROM crawl_metrics")
    total_bytes = cursor.fetchone()[0] or 0
    total_db_size_mb = total_bytes / 1024 / 1024
    
    conn.close()
    return {
        'total_rows': total_rows,
        'total_db_size_mb': total_db_size_mb,
        'domain_stats': {}
    }

def main():
    # Initialize metrics system
    metrics = reset_metrics()

    # Extract unique domains from seed URLs
    domains = set()
    for seed_url in SEED_URLS:
        domain = urlparse(seed_url).netloc
        domains.add(domain)

    # Initialize centralized MySQL schema (idempotent)
    initialize_db()

    # Create frontier
    frontier = Frontier()

    # Seed frontier
    for seed_url in SEED_URLS:
        frontier.enqueue(seed_url, None, 0)  # discovered_from=None, depth=0 for seeds

    # Record crawl start time
    start_time = time.time()

    # Start workers
    workers = []
    num_workers = MIN_WORKERS
    for i in range(num_workers):
        worker = Worker(frontier, name=f"Worker-{i}")
        worker.start()
        workers.append(worker)

    # Wait for completion with dynamic scaling (max 5 minutes timeout)
    last_log_time = time.time()
    idle_counter = 0
    max_timeout = 300  # 5 minutes
    start_wait = time.time()
    
    while not frontier.is_empty():
        time.sleep(1)
        current_time = time.time()
        elapsed_wait = current_time - start_wait
        
        # Check if timeout exceeded
        if elapsed_wait > max_timeout:
            print(f"\n[TIMEOUT] Crawl exceeded {max_timeout}s. Forcing shutdown...\n")
            break
        
        # Print progress summary every 30 seconds
        if current_time - last_log_time > 30:
            frontier_stats = frontier.get_stats()
            db_stats = get_db_stats()
            metrics.print_progress_summary(frontier_stats, db_stats)
            print(f"[Elapsed: {int(elapsed_wait)}s/{max_timeout}s]")
            last_log_time = current_time
            
            # Track idle state (no queue movement)
            if frontier_stats['queue_size'] == 0 and frontier_stats['in_progress_count'] > 0:
                idle_counter += 1
                if idle_counter > 3:  # 3 cycles (90 seconds) with no progress
                    print(f"\n[STALLED] {frontier_stats['in_progress_count']} URLs stuck in progress. Force shutting down...\n")
                    break
            else:
                idle_counter = 0
        
        # Dynamic worker scaling: scale up based on queue size, max 20 workers
        current_queue_size = frontier.get_stats()['queue_size']
        if current_queue_size > 100 and len(workers) < MAX_WORKERS:
            new_worker = Worker(frontier, name=f"Worker-{len(workers)}")
            new_worker.start()
            workers.append(new_worker)
            print(f"\n Scaled up to {len(workers)} workers due to queue size {current_queue_size}\n")

    # Record crawl end time
    end_time = time.time()

    # Stop workers
    for worker in workers:
        worker.stop()
        worker.join()

    # Flush DB writer queue before summary
    try:
        from crawler.worker import _db_writer
        if _db_writer:
            print("[Main] Flushing DB writer queue...")
            _db_writer.stop()
            time.sleep(2)  # Give writer time to flush remaining batches
            print("[Main] DB writer flushed successfully")
    except Exception as e:
        print(f"[Main] Warning: Could not flush DB writer: {e}")

    # Collect CPU and memory stats from workers before doing anything else
    metrics.collect_worker_stats(workers)

    # RETRY MECHANISM: Retry timeout failures
    print("\n" + "="*100)
    print("CHECKING FOR TIMEOUT FAILURES TO RETRY")
    print("="*100)

    # Collect timeout failures from metrics
    timeout_urls = [record for record in metrics.url_records
                    if record['status'] == 'failed' and 'timeout' in record.get('error_reason', '').lower()]

    if timeout_urls:
        print(f"\n Found {len(timeout_urls)} timeout failures. Retrying with 2 workers...\n")

        # Re-enqueue timeout URLs
        for record in timeout_urls:
            frontier.enqueue(record['url'], record.get('discovered_from'), 0)

        # Start 2 retry workers with is_retry_worker=True flag
        retry_workers = []
        for i in range(2):
            worker = Worker(frontier, name=f"Retry-{i}", is_retry_worker=True)
            worker.start()
            retry_workers.append(worker)

        # Wait for retry completion
        while not frontier.is_empty():
            time.sleep(1)

        # Stop retry workers
        for worker in retry_workers:
            worker.stop()
            worker.join()

        print(f"\n Retry phase completed\n")
    else:
        print(f"\n No timeout failures found. Skipping retry phase.\n")

    # Print comprehensive final summary
    db_stats = get_db_stats()
    mem_stats = frontier.get_memory_stats()
    metrics.print_final_summary(db_stats, mem_stats)


if __name__ == "__main__":
    main()
