#!/usr/bin/env python3
"""
Entry point for the web crawler (MySQL version).
- Validates MySQL connectivity
- Fetches crawl targets from DB (sites table)
- Creates a crawl job per site
- Seeds the frontier
- Runs workers with adaptive scaling
- Tracks crawl_jobs lifecycle
- Prints crawl summary per site
"""

from dotenv import load_dotenv
load_dotenv()

import time
import uuid
from crawler.frontier import Frontier
from crawler.worker import Worker
from crawler.normalizer import normalize_url
from crawler.storage.db import (
    check_db_health,
    fetch_enabled_sites,
    insert_crawl_job,
    complete_crawl_job,
    fail_crawl_job,
)
import os
CRAWL_MODE = os.getenv("CRAWL_MODE", "CRAWL").upper()

assert CRAWL_MODE in ("BASELINE", "CRAWL", "COMPARE"), (
    f"Invalid CRAWL_MODE={CRAWL_MODE}. "
    "Must be BASELINE, CRAWL, or COMPARE."
)


from crawler.worker import BLOCK_REPORT


# --- CONFIG ---
INITIAL_WORKERS = 5
MAX_WORKERS = 20
SCALE_THRESHOLD = 100  # queue size


def main():
    # -------------------------------------------------
    # 1. Hard DB safety check (fail fast)
    # -------------------------------------------------
    try:
        if not check_db_health():
            print("ERROR: MySQL health check failed. Exiting.")
            return
    except Exception as e:
        print(f"ERROR: MySQL unavailable: {e}")
        return

    print("MySQL health check passed.")

    # -------------------------------------------------
    # 2. Fetch crawl targets from DB
    # -------------------------------------------------
    sites = fetch_enabled_sites()
    if not sites:
        print("No enabled sites found in DB. Exiting.")
        return

    print(f"Found {len(sites)} enabled site(s) to crawl.")

    # -------------------------------------------------
    # 3. Crawl each site independently
    # -------------------------------------------------
    for site in sites:
        # Defensive: ensure dictionary keys match exactly
        assert "siteid" in site, "Key 'siteid' missing from site dict. Check fetch_enabled_sites() output."
        siteid = site["siteid"]
        custid = site["custid"]

        # Raw URL from DB may contain stray whitespace or missing scheme.
        raw_url = site["url"]
        start_url = normalize_url(raw_url.strip() if isinstance(raw_url, str) else raw_url)

        # Defensive: ensure siteid is not None
        assert siteid is not None, "siteid is None — check sites table data"

        job_id = str(uuid.uuid4())

        print("\n" + "=" * 60)
        print(f"Starting crawl job {job_id}")
        print(f"Customer ID : {custid}")
        print(f"Site ID     : {siteid}")
        print(f"Seed URL    : {start_url}")
        print("=" * 60)

        try:
            # -------------------------------------------------
            # 4. Register crawl job (lifecycle START)
            # -------------------------------------------------
            insert_crawl_job(
                job_id=job_id,
                custid=custid,
                siteid=siteid,
                start_url=start_url,
            )

            # -------------------------------------------------
            # 5. Initialize frontier
            # -------------------------------------------------
            frontier = Frontier()
            frontier.enqueue(
                start_url,
                discovered_from=None,
                depth=0,
            )

            # -------------------------------------------------
            # 6. Start workers
            # -------------------------------------------------
            workers = []
            start_time = time.time()

            # Worker expects a map → keep interface intact
            siteid_map = {siteid: siteid}

            for i in range(INITIAL_WORKERS):
                w = Worker(
                    frontier=frontier,
                    name=f"Worker-{i}",
                    custid=custid,
                    siteid_map=siteid_map,
                    job_id=job_id,
                    crawl_mode=CRAWL_MODE,
                )
                w.start()
                workers.append(w)

            print(f"Started {len(workers)} workers.")

            # -------------------------------------------------
            # 7. Crawl loop with adaptive scaling
            # -------------------------------------------------
            while not frontier.is_empty():
                time.sleep(1)

                stats = frontier.get_stats()
                qsize = stats["queue_size"]

                if qsize > SCALE_THRESHOLD and len(workers) < MAX_WORKERS:
                    w = Worker(
                        frontier=frontier,
                        name=f"Worker-{len(workers)}",
                        custid=custid,
                        siteid_map=siteid_map,
                        job_id=job_id,
                        crawl_mode=CRAWL_MODE,
                    )
                    w.start()
                    workers.append(w)
                    print(f"Scaled up → {len(workers)} workers (queue={qsize})")

            # -------------------------------------------------
            # 8. Shutdown workers cleanly
            # -------------------------------------------------
            for w in workers:
                w.stop()

            for w in workers:
                w.join()

            end_time = time.time()

            # -------------------------------------------------
            # 9. Mark crawl job as COMPLETED
            # -------------------------------------------------
            pages_crawled = frontier.get_stats()["visited_count"]

            complete_crawl_job(
                job_id=job_id,
                pages_crawled=pages_crawled,
            )

            # -------------------------------------------------
            # 10. Print crawl summary
            # -------------------------------------------------
            duration = end_time - start_time
            stats = frontier.get_stats()

            print("\n" + "-" * 60)
            print("CRAWL COMPLETED")
            print("-" * 60)
            print(f"Job ID            : {job_id}")
            print(f"Customer ID       : {custid}")
            print(f"Site ID           : {siteid}")
            print(f"Seed URL          : {start_url}")
            print(f"Total URLs visited: {stats['visited_count']}")
            print(f"Crawl duration    : {duration:.2f} seconds")
            print(f"Workers used      : {len(workers)}")
            print(f"Routing graph     : {len(frontier.routing_graph)} nodes")
            print("-" * 60)

        except Exception as e:
            # -------------------------------------------------
            # 11. Mark crawl job as FAILED
            # -------------------------------------------------
            fail_crawl_job(job_id, str(e))
            print(f"ERROR: Crawl job {job_id} failed: {e}")
            raise

    print("\nAll site crawls completed successfully.")


if __name__ == "__main__":
    # Run the crawler and then, if any URLs were blocked by the worker-level
    # rules, print a concise report so we can see if over-aggressive blocking
    # is the reason no URLs are being crawled.
    main()

    if BLOCK_REPORT:
        print("\n" + "=" * 60)
        print("BLOCKED URL REPORT")
        print("=" * 60)

        total = 0
        for block_type, urls in BLOCK_REPORT.items():
            print(f"\n[{block_type}]  {len(urls)} URLs blocked")
            for u in urls[:20]:
                print(f"  - {u}")
            if len(urls) > 20:
                print(f"  ... ({len(urls) - 20} more)")
            total += len(urls)

        print("\nTotal blocked URLs:", total)
        print("=" * 60)
