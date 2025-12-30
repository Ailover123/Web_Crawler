"""
Entry point for the web crawler.
Initializes DB, seeds frontier, starts workers, waits for completion.
Computes per-domain crawl statistics, prints summaries, and dumps debug JSON.
"""

from crawler.storage.db import initialize_db, DB_PATH, get_connection
from crawler.config import SEED_URLS
from crawler.frontier import Frontier
from crawler.worker import Worker
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

    # Compute per-domain statistics
    # conn = get_connection()
    # cursor = conn.cursor()

    # # Get all unique domains
    # cursor.execute("SELECT DISTINCT domain FROM urls")
    # domains = [row[0] for row in cursor.fetchall()]

    domain_summaries = {}
    overall_duration = end_time - start_time  # Overall crawl duration

    # New logic for analysis
    domains = set()
    for url in frontier.discovered:
        domain = urlparse(url).netloc
        domains.add(domain)

    blocked_substrings = [
        "wp-content",
        "wp-includes",
        "wp-json",
        "elementor",
        "admin",
        "login",
        "assets",
        "uploads",
        "api"
    ]

    for domain in domains:
        urls_for_domain = [url for url in frontier.discovered if urlparse(url).netloc == domain]
        total_discovered = len(urls_for_domain)

        substring_breakdown = {}
        top_examples = {}
        no_blocked_count = 0
        no_blocked_examples = []

        for url in urls_for_domain:
            url_lower = url.lower()
            matched = False
            for sub in blocked_substrings:
                if sub in url_lower:
                    substring_breakdown[sub] = substring_breakdown.get(sub, 0) + 1
                    if sub not in top_examples:
                        top_examples[sub] = []
                    if len(top_examples[sub]) < 2:
                        top_examples[sub].append(url)
                    matched = True
            if not matched:
                no_blocked_count += 1
                if len(no_blocked_examples) < 2:
                    no_blocked_examples.append(url)

        substring_breakdown["no_blocked_substring"] = no_blocked_count
        top_examples["no_blocked_substring"] = no_blocked_examples

        domain_summaries[domain] = {
            "domain": domain,
            "total_discovered_urls": total_discovered,
            "substring_breakdown": substring_breakdown,
            "top_examples": top_examples
        }

    # Print the JSON reports
    for domain, summary in domain_summaries.items():
        print(json.dumps(summary, indent=4))

    # Generate combined domain analysis JSON
        combined_analysis = generate_combined_domain_analysis(frontier)
    with open('combined_domain_analysis.json', 'w') as f:
        json.dump(combined_analysis, f, indent=4)

    # Output routing graph JSON
    with open('routing_graph.json', 'w') as f:
        json.dump(frontier.routing_graph, f, indent=4)

    def generate_combined_domain_analysis(frontier):
        """
        Generate a combined analysis of all discovered URLs grouped by domain and type.
        Returns a dict with domain as key and type breakdown as value.
        """
        from urllib.parse import urlparse
        from crawler.parser import classify_url
        domain_type_summary = {}
        for url in frontier.discovered:
            domain = urlparse(url).netloc
            types = classify_url(url)
            if domain not in domain_type_summary:
                domain_type_summary[domain] = {}
            for t in types:
                if t not in domain_type_summary[domain]:
                    domain_type_summary[domain][t] = {"count": 0, "urls": []}
                domain_type_summary[domain][t]["count"] += 1
                if len(domain_type_summary[domain][t]["urls"]) < 10:
                    domain_type_summary[domain][t]["urls"].append({"sr": len(domain_type_summary[domain][t]["urls"]) + 1, "url": url})
        return {"types_summary": domain_type_summary}
if __name__ == "__main__":
    main()
