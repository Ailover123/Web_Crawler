"""
Crawl-only audit run script.
Performs a single crawl, classifies outcomes, and outputs two JSON files.
"""

from crawler.storage.db import initialize_db, DB_PATH, get_connection
from crawler.config import SEED_URLS
from crawler.frontier import Frontier
from crawler.worker import Worker
import time
import os
import json
import uuid
from datetime import datetime

def main():
    crawl_id = str(uuid.uuid4())
    started_at = datetime.utcnow().isoformat() + "Z"

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

    # Start workers
    workers = []
    num_workers = 5
    for i in range(num_workers):
        worker = Worker(frontier, name=f"Worker-{i}")
        worker.start()
        workers.append(worker)

    # Wait for completion
    while not frontier.is_empty():
        time.sleep(1)

    finished_at = datetime.utcnow().isoformat() + "Z"

    # Stop workers
    for worker in workers:
        worker.stop()
        worker.join()

    # Collect data from DB
    conn = get_connection()
    cursor = conn.cursor()

    # Get all URLs
    cursor.execute("""
        SELECT url, domain, status, http_status, content_type, response_size, fetch_time_ms, error_type, discovered_from, depth, crawled_at
        FROM urls
    """)
    rows = cursor.fetchall()

    # Group by domain
    domains_data = {}
    for row in rows:
        url, domain, status, http_status, content_type, response_size, fetch_time_ms, error_type, discovered_from, depth, crawled_at = row
        if domain not in domains_data:
            domains_data[domain] = {
                'urls': [],
                'summary': {
                    'domain': domain,
                    'total_urls_attempted': 0,
                    'success_count': 0,
                    'ignored_count': 0,
                    'fetch_failed_count': 0,
                    'total_response_size_bytes': 0,
                    'total_fetch_time_ms': 0,
                    'average_fetch_time_ms': 0
                }
            }

        domains_data[domain]['urls'].append({
            'url': url,
            'discovered_from': discovered_from,
            'depth': depth,
            'status': status,
            'http_status': http_status,
            'content_type': content_type,
            'response_size': response_size,
            'fetch_time_ms': fetch_time_ms,
            'error_type': error_type,
            'timestamp': crawled_at
        })

        domains_data[domain]['summary']['total_urls_attempted'] += 1
        if status == 'success':
            domains_data[domain]['summary']['success_count'] += 1
            domains_data[domain]['summary']['total_response_size_bytes'] += response_size or 0
            domains_data[domain]['summary']['total_fetch_time_ms'] += fetch_time_ms or 0
        elif status == 'ignored':
            domains_data[domain]['summary']['ignored_count'] += 1
        elif status == 'fetch_failed':
            domains_data[domain]['summary']['fetch_failed_count'] += 1

    # Calculate averages
    for domain, data in domains_data.items():
        summary = data['summary']
        if summary['success_count'] > 0:
            summary['average_fetch_time_ms'] = summary['total_fetch_time_ms'] / summary['success_count']
        else:
            summary['average_fetch_time_ms'] = 0

        # Consistency check
        if summary['total_urls_attempted'] != summary['success_count'] + summary['ignored_count'] + summary['fetch_failed_count']:
            raise RuntimeError(f"DB consistency check failed for domain {domain}")

    conn.close()

    # Create fetch_failures.json
    fetch_failures = {
        'crawl_id': crawl_id,
        'started_at': started_at,
        'finished_at': finished_at,
        'domains': {}
    }
    for domain, data in domains_data.items():
        fetch_failures['domains'][domain] = {
            'urls': [u for u in data['urls'] if u['status'] == 'fetch_failed']
        }

    with open('fetch_failures.json', 'w') as f:
        json.dump(fetch_failures, f, indent=4)

    # Create db_summary.json
    db_summary = {
        'crawl_id': crawl_id,
        'started_at': started_at,
        'finished_at': finished_at,
        'domains': {}
    }
    for domain, data in domains_data.items():
        db_summary['domains'][domain] = {
            'summary': data['summary']
        }

    with open('db_summary.json', 'w') as f:
        json.dump(db_summary, f, indent=4)

    # Print per-domain summaries
    for domain, data in domains_data.items():
        summary = data['summary']
        print(f"Domain: {domain}")
        print(f"  Total URLs attempted: {summary['total_urls_attempted']}")
        print(f"  Success count: {summary['success_count']}")
        print(f"  Ignored count: {summary['ignored_count']}")
        print(f"  Fetch failed count: {summary['fetch_failed_count']}")
        print(f"  Total response size bytes: {summary['total_response_size_bytes']}")
        print(f"  Total fetch time ms: {summary['total_fetch_time_ms']}")
        print(f"  Average fetch time ms: {summary['average_fetch_time_ms']:.2f}")
        print()

if __name__ == "__main__":
    main()
