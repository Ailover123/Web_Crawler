"""
Entry point for the web crawler.
Initializes DB, seeds frontier, starts workers, waits for completion.
Computes per-domain crawl statistics, prints summaries, and dumps debug JSON.
"""

from crawler.storage.db import initialize_db, get_connection, fetch_active_sites
from crawler.config import MIN_WORKERS, MAX_WORKERS
from crawler.frontier import Frontier
from crawler.worker import Worker, finalize_site_outputs
from crawler.metrics import reset_metrics, get_metrics
from crawler.policy import URLPolicy
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
    
    # TEMP DISABLED: crawl_metrics not used for MVP
    # cursor.execute("SELECT SUM(size_bytes) FROM crawl_metrics")
    # total_bytes = cursor.fetchone()[0] or 0
    # total_db_size_mb = total_bytes / 1024 / 1024
    total_db_size_mb = 0  # In-memory metrics only
    
    conn.close()
    return {
        'total_rows': total_rows,
        'total_db_size_mb': total_db_size_mb,
        'domain_stats': {}
    }


def _print_policy_summary(siteid, root_url):
    stats = URLPolicy.get_stats()
    print(f"=== POLICY SUMMARY FOR siteid={siteid} ({root_url}) ===")
    for key in sorted(stats.keys()):
        print(f"{key}: {stats[key]}")
    print("")

def main():
    # Initialize metrics system
    metrics = reset_metrics()

    # Initialize centralized MySQL schema (idempotent)
    initialize_db()

    # Fetch active sites from database
    active_sites = fetch_active_sites()
    if not active_sites:
        print("[CRAWL] No active sites found in database. Skipping crawl.")
        return

    print(f"[CRAWL] Found {len(active_sites)} active site(s) to crawl")

    # Crawl each site separately with its own frontier
    for siteid, root_url, custid, app_type in active_sites:
        print(f"\n[CRAWL] ===== Starting crawl for siteid={siteid}: {root_url} =====\n")
        URLPolicy.reset_stats()

        # Create fresh frontier for this site
        frontier = Frontier()

        # Register site scope for this siteid (used for scope validation during crawl)
        frontier.set_site_scope(siteid, root_url)

        # Seed frontier with the root URL for this site
        frontier.enqueue(root_url, discovered_from=None, depth=0, siteid=siteid)

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
                print(f"\n[TIMEOUT] Crawl for siteid={siteid} exceeded {max_timeout}s. Forcing shutdown...\n")
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

        # Stop workers for this site
        print(f"[CRAWL] Stopping workers for siteid={siteid}...")
        for worker in workers:
            worker.stop()
            worker.join()

        finalize_site_outputs(siteid)
        print(f"[CRAWL] ===== Completed crawl for siteid={siteid}: {root_url} =====\n")
        
        # Capture and store policy stats for this domain
        policy_stats = URLPolicy.get_stats()
        metrics.record_policy_stats_for_domain(root_url, policy_stats)
        _print_policy_summary(siteid, root_url)

    print("\n[CRAWL] ===== ALL SITES CRAWLED =====\n")

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

    # Print comprehensive final summary
    db_stats = get_db_stats()
    print("\n[Main] Crawl completed. Summary:")
    print(f"  Total rows in sites table: {db_stats['total_rows']}")
    print(f"  Total DB size: {db_stats['total_db_size_mb']:.2f} MB")

    # Write per-domain stats to JSON file
    stats_file = metrics.write_domain_stats_to_json()
    if stats_file:
        print(f"[Main] Per-domain statistics saved to: {stats_file}")


if __name__ == "__main__":
    main()
