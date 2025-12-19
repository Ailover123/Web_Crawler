# Responsibilities:
# - initialize database
# - initialize crawl queue
# - orchestrate crawling
# - create baselines when missing
# - detect defacement when baseline exists
# - store forensic diff evidence

from crawler.config import SEED_URLS, DEPTH_LIMIT
from crawler.queue import CrawlQueue
from crawler.fetcher import fetch
from crawler.parser import parse_html
from crawler.normalizer import normalize_html
from crawler.hasher import hash_content
from crawler.detection.detector import detect_defacement
from crawler.storage.db import initialize_db
from crawler.storage.url_store import insert_url, update_crawl_metadata
from crawler.storage.baseline_store import store_baseline
from crawler.storage.diff_store import store_diff

def main():
    # 1. Initialize database schema (safe to call multiple times)
    initialize_db()

    # 2. Initialize crawl queue with depth limit
    queue = CrawlQueue(max_depth=DEPTH_LIMIT)

    # 3. Seed the queue and URL inventory
    for seed_url in SEED_URLS:
        insert_url(seed_url, crawl_depth=0)
        queue.enqueue(seed_url, depth=0)

    # 4. Crawl loop (Breadth-First)
    while not queue.is_empty():
        item = queue.dequeue()
        if item is None:
            break

        url, depth = item
        print(f"[CRAWL] {url} (depth={depth})")

        # 5. Fetch HTTP response
        response = fetch(url)
        if response is None:
            # Fetch failure is not defacement
            update_crawl_metadata(url, status="fetch_failed")
            continue

        # 6. Parse HTML and extract structure
        soup, discovered_links, script_sources = parse_html(
            response.text, base_url=url
        )

        # 7. Normalize HTML to remove noise
        normalized_html = normalize_html(soup)

        # 8. Hash normalized HTML (integrity fingerprint)
        html_hash = hash_content(normalized_html)

        observed_data = {
            "html_hash": html_hash,
            "script_sources": script_sources,
        }

        # 9. Detect defacement OR create baseline
        detection_result = detect_defacement(url, observed_data)

        if detection_result is None:
            # No baseline exists → create baseline
            store_baseline(
                url=url,
                html_hash=html_hash,
                script_sources=script_sources,
            )
            print(f"[BASELINE CREATED] {url}")

        elif detection_result["defaced"]:
            # Defacement detected → store forensic evidence
            store_diff(
                url=url,
                baseline_hash=detection_result["baseline_hash"],
                observed_hash=detection_result["observed_hash"],
                diff_summary={
                    "html_changed": detection_result["html_changed"],
                    "scripts_added": detection_result["scripts_added"],
                    "scripts_removed": detection_result["scripts_removed"],
                },
                severity=detection_result["severity"],
            )
            print(f"[DEFACEMENT DETECTED] {url}")

        else:
            print(f"[OK] No change detected for {url}")

        # 10. Enqueue newly discovered links
        for link in discovered_links:
            queue.enqueue(link, depth + 1)

        # 11. Update crawl metadata
        update_crawl_metadata(url, status="crawled")
 
if __name__ == "__main__":
    main()
