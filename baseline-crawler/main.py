"""
Entry point for the web crawler.
Initializes DB, seeds frontier, starts workers, waits for completion.
Computes per-domain crawl statistics, prints summaries, and dumps debug JSON.
"""

from crawler.storage.db import initialize_db, DB_PATH, get_connection
from crawler.config import SEED_URLS
from crawler.frontier import Frontier
from crawler.worker import Worker
from combined_domain_analysis import generate_combined_domain_analysis
import time
import os
import json
from urllib.parse import urlparse
from threading import Lock
def main():
    # Delete existing DB file if it exists
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    # Initialize DB
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

    # No per-domain statistics printing in analysis mode

    # Generate combined domain analysis JSON
        combined_analysis = generate_combined_domain_analysis(frontier)
    with open('combined_domain_analysis.json', 'w') as f:
        json.dump(combined_analysis, f, indent=4)

    # Output routing graph JSON
    with open('routing_graph.json', 'w') as f:
        json.dump(frontier.routing_graph, f, indent=4)

    
if __name__ == "__main__":
    main()
