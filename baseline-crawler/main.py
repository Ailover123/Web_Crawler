"""
Entry point for the web crawler.
Initializes DB, seeds frontier, starts workers, waits for completion.
Computes per-domain crawl statistics, prints summaries, and dumps debug JSON.
"""

from crawler.storage.db import initialize_db, get_db_path, get_connection, initialize_failed_db
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

def get_db_stats(domains):
    """Get storage stats for all domain DBs."""
    total_rows = 0
    total_db_size = 0
    domain_stats = {}
    for domain in domains:
        conn = get_connection(domain)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM crawl_data")
        rows = cursor.fetchone()[0]
        db_path = get_db_path(domain)
        db_size_mb = os.path.getsize(db_path) / 1024 / 1024 if os.path.exists(db_path) else 0
        domain_stats[domain] = {'rows': rows, 'db_size_mb': db_size_mb}
        total_rows += rows
        total_db_size += db_size_mb
        conn.close()
    return {
        'total_rows': total_rows,
        'total_db_size_mb': total_db_size,
        'domain_stats': domain_stats
    }

def main():
    # Initialize metrics system
    metrics = reset_metrics()

    # Extract unique domains from seed URLs
    domains = set()
    for seed_url in SEED_URLS:
        domain = urlparse(seed_url).netloc
        domains.add(domain)

    # Initialize domain-specific databases
    old_runs_dir = os.path.join(os.path.dirname(get_db_path("dummy")), "old_runs")
    os.makedirs(old_runs_dir, exist_ok=True)
    for domain in domains:
        db_path = get_db_path(domain)
        if os.path.exists(db_path):
            # Move old DB to old_runs with timestamp
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            old_db_name = f"data_{domain}_{timestamp}.db"
            old_db_path = os.path.join(old_runs_dir, old_db_name)
            os.rename(db_path, old_db_path)
            print(f"Moved old DB for {domain} to {old_db_path}")
        initialize_db(domain)

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

    # Wait for completion with dynamic scaling
    last_log_time = time.time()
    while not frontier.is_empty():
        time.sleep(1)
        current_time = time.time()
        # Print progress summary every 30 seconds
        if current_time - last_log_time > 30:
            frontier_stats = frontier.get_stats()
            db_stats = get_db_stats(domains)
            metrics.print_progress_summary(frontier_stats, db_stats)
            last_log_time = current_time
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

    # Collect CPU and memory stats from workers before doing anything else
    metrics.collect_worker_stats(workers)

    # RETRY MECHANISM: Retry timeout failures
    print("\n" + "="*100)
    print("CHECKING FOR TIMEOUT FAILURES TO RETRY")
    print("="*100)

    # Collect timeout failures from metrics
    timeout_urls = [record for record in metrics.url_records
                    if record['status'] == 'failed' and 'timeout' in record.get('error', '').lower()]

    if timeout_urls:
        print(f"\n Found {len(timeout_urls)} timeout failures. Retrying with 2 workers...\n")

        # Re-enqueue timeout URLs
        for record in timeout_urls:
            frontier.enqueue(record['url'], record.get('discovered_from'), 0)

        # Start 2 retry workers
        retry_workers = []
        for i in range(2):
            worker = Worker(frontier, name=f"Retry-{i}")
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
    db_stats = get_db_stats(domains)
    mem_stats = frontier.get_memory_stats()
    metrics.print_final_summary(db_stats, mem_stats)


if __name__ == "__main__":
    main()
