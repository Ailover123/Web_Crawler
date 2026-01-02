"""
Entry point for the web crawler.
Initializes DB, seeds frontier, starts workers, waits for completion.
Computes per-domain crawl statistics, prints summaries, and dumps debug JSON.
"""

from crawler.storage.db import initialize_db, get_db_path, get_connection
from crawler.config import SEED_URLS
from crawler.frontier import Frontier
from crawler.worker import Worker
from combined_domain_analysis import generate_combined_domain_analysis
import time
import os
import json
import datetime
from urllib.parse import urlparse
from threading import Lock
def main():
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
    num_workers = 5  # Start with 5 workers for concurrency
    for i in range(num_workers):
        worker = Worker(frontier, name=f"Worker-{i}")
        worker.start()
        workers.append(worker)

    # Wait for completion with dynamic scaling
    while not frontier.is_empty():
        time.sleep(1)
        # Dynamic worker scaling: scale up based on queue size, max 20 workers
        current_queue_size = frontier.get_stats()['queue_size']
        if current_queue_size > 100 and len(workers) < 20:
            new_worker = Worker(frontier, name=f"Worker-{len(workers)}")
            new_worker.start()
            workers.append(new_worker)
            print(f"Scaled up to {len(workers)} workers due to queue size {current_queue_size}")

    # Record crawl end time
    end_time = time.time()

    # Stop workers
    for worker in workers:
        worker.stop()
        worker.join()

    # Calculate and display crawl statistics
    crawl_duration = end_time - start_time
    stats = frontier.get_stats()

    print("\n" + "="*60)
    print("CRAWL COMPLETED")
    print("="*60)
    print(f"Total crawl time: {crawl_duration:.2f} seconds")
    print(f"URLs visited: {stats['visited_count']}")
    print(f"URLs in progress: {stats['in_progress_count']}")
    print(f"URLs remaining in queue: {stats['queue_size']}")
    print(f"Total workers used: {len(workers)}")
    print(f"Routing graph size: {len(frontier.routing_graph)} nodes")

    # Per-domain statistics
    print("\nPer-Domain Statistics:")
    for domain in domains:
        conn = get_connection(domain)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM crawl_data")
        url_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM crawl_data WHERE fetch_status = 200")
        success_count = cursor.fetchone()[0]
        conn.close()
        print(f"  {domain}: {url_count} URLs crawled, {success_count} successful")

    print("="*60)

    # Generate combined domain analysis JSON
    combined_analysis = generate_combined_domain_analysis(frontier)
    with open('combined_domain_analysis.json', 'w') as f:
        json.dump(combined_analysis, f, indent=4)

    # Output routing graph JSON
    with open('routing_graph.json', 'w') as f:
        json.dump(frontier.routing_graph, f, indent=4)

    
if __name__ == "__main__":
    main()
