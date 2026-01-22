#!/usr/bin/env python3
"""
Entry point for the web crawler.
Uses queue.join() for deterministic crawl completion.
"""

from dotenv import load_dotenv
load_dotenv()

import time
import uuid
import os
import requests

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

from crawler.worker import BLOCK_REPORT

CRAWL_MODE = os.getenv("CRAWL_MODE", "CRAWL").upper()
assert CRAWL_MODE in ("BASELINE", "CRAWL", "COMPARE")

INITIAL_WORKERS = 5
MAX_WORKERS = 20
SCALE_THRESHOLD = 100


# ============================================================
# SEED URL RESOLUTION (FOR FETCHING ONLY)
# ============================================================

def resolve_seed_url(raw_url: str) -> str:
    """
    Resolve a working URL for crawling.
    This does NOT change DB identity.
    """
    raw = raw_url.strip()

    # Try without / and with /
    candidates = (
        [raw.rstrip("/"), raw]
        if raw.endswith("/")
        else [raw, raw + "/"]
    )

    for u in candidates:
        try:
            r = requests.get(
                u if u.startswith(("http://", "https://")) else "https://" + u,
                timeout=8,
                allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if r.status_code < 400:
                return r.url
        except Exception:
            continue

    # Last-resort fallback
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    return raw


# ============================================================
# MAIN
# ============================================================

def main():
    # ---------------- DB CHECK ----------------
    if not check_db_health():
        print("ERROR: MySQL health check failed.")
        return

    print("MySQL health check passed.")

    sites = fetch_enabled_sites()
    if not sites:
        print("No enabled sites found.")
        return

    print(f"Found {len(sites)} enabled site(s).")

    # ---------------- PER SITE ----------------
    for site in sites:
        siteid = site["siteid"]
        custid = site["custid"]

        # 🔒 EXACT value from sites table (DB identity)
        original_site_url = site["url"].strip()

        # 🌐 Resolve + normalize ONLY for crawling
        resolved_seed = resolve_seed_url(original_site_url)
        start_url = normalize_url(resolved_seed)

        job_id = str(uuid.uuid4())

        print("\n" + "=" * 60)
        print(f"Starting crawl job {job_id}")
        print(f"Customer ID : {custid}")
        print(f"Site ID     : {siteid}")
        print(f"Seed URL    : {start_url}")
        print(f"DB URL      : {original_site_url}")
        print("=" * 60)

        try:
            insert_crawl_job(
                job_id=job_id,
                custid=custid,
                siteid=siteid,
                start_url=original_site_url,  # ✅ store EXACT value
            )

            frontier = Frontier()
            frontier.enqueue(start_url, None, 0)

            workers = []
            start_time = time.time()

            siteid_map = {siteid: siteid}

            for i in range(INITIAL_WORKERS):
                w = Worker(
                    frontier=frontier,
                    name=f"Worker-{i}",
                    custid=custid,
                    siteid_map=siteid_map,
                    job_id=job_id,
                    crawl_mode=CRAWL_MODE,
                    seed_url=start_url,                 # crawl truth
                    original_site_url=original_site_url,  # DB truth
                )
                w.start()
                workers.append(w)

            print(f"Started {len(workers)} workers.")

            # 🔒 Deterministic completion
            frontier.queue.join()

            # ---------------- SHUTDOWN ----------------
            for w in workers:
                w.stop()
            for w in workers:
                w.join()

            duration = time.time() - start_time
            stats = frontier.get_stats()

            complete_crawl_job(
                job_id=job_id,
                pages_crawled=stats["visited_count"],
            )

            print("\n" + "-" * 60)
            print("CRAWL COMPLETED")
            print("-" * 60)
            print(f"Job ID            : {job_id}")
            print(f"Customer ID       : {custid}")
            print(f"Site ID           : {siteid}")
            print(f"Seed URL (crawl)  : {start_url}")
            print(f"URL (DB)          : {original_site_url}")
            print(f"Total URLs visited: {stats['visited_count']}")
            print(f"Crawl duration    : {duration:.2f} seconds")
            print(f"Workers used      : {len(workers)}")
            print("-" * 60)

        except Exception as e:
            fail_crawl_job(job_id, str(e))
            print(f"ERROR: Crawl job {job_id} failed: {e}")
            raise

    print("\nAll site crawls completed successfully.")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    main()

    if BLOCK_REPORT:
        print("\n" + "=" * 60)
        print("BLOCKED URL REPORT")
        print("=" * 60)
        for block_type, data in BLOCK_REPORT.items():
            try:
                count = len(data)
                urls = list(data)
            except Exception:
                count = 0
                urls = []

            print(f"[{block_type}] {count} URLs blocked")
            for u in urls:
                print(f"  - {u}")
        print("=" * 60)
