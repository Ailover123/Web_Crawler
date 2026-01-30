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
import os
import requests
import argparse # Added argparse

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
from crawler.storage.mysql import fetch_site_info_by_baseline_id # Added import
from crawler.baseline_worker import BaselineWorker

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

    # ---------------- ARG PARSE ----------------
    parser = argparse.ArgumentParser(description="Defacement Crawler / Baseline Tool")
    parser.add_argument("--siteid", type=int, help="Run only for this specific SITE ID")
    parser.add_argument("--baseline_id", type=str, help="Run only for this specific BASELINE ID")
    args = parser.parse_args()

    # ---------------- DB CHECK ----------------
    if not check_db_health():
        print("ERROR: MySQL health check failed.")
        return

    print("MySQL health check passed.")

    sites = fetch_enabled_sites()
    if not sites:
        print("No enabled sites found.")
        return


    # ---------------- FILTER SITES ----------------
    
    target_urls = None
    target_siteid = args.siteid

    # If baseline_id provided, it overrides siteid and sets specific target
    if args.baseline_id:
        info = fetch_site_info_by_baseline_id(args.baseline_id)
        if not info:
             print(f"ERROR: Baseline ID '{args.baseline_id}' not found in defacement_sites.")
             return
        target_siteid = info["siteid"]
        target_urls = [info["url"]]
        print(f"--> Targeting specific Baseline ID: {args.baseline_id} (Site {target_siteid})")

    if target_siteid:
        # Filter the list of sites
        sites = [s for s in sites if s["siteid"] == target_siteid]
        if not sites:
            print(f"Site ID {target_siteid} not found or not enabled.")
            return
        print(f"Filtered to single site ID: {target_siteid}")

    print(f"Found {len(sites)} enabled site(s) to process.")

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
                start_url=original_site_url,
            )

            start_time = time.time()

            # ====================================================
            # BASELINE MODE (NO CRAWLING, NO WORKERS)
            # ====================================================
            if CRAWL_MODE == "BASELINE":
                print("[MODE] BASELINE (offline, DB-driven)")

                BaselineWorker(
                    custid=custid,
                    siteid=siteid,
                    seed_url=start_url,
                    target_urls=target_urls, # Pass the filter
                ).run()

                duration = time.time() - start_time

                complete_crawl_job(
                    job_id=job_id,
                    pages_crawled=0,
                )

                print("\n" + "-" * 60)
                print("BASELINE COMPLETED")
                print("-" * 60)
                print(f"Job ID        : {job_id}")
                print(f"Customer ID   : {custid}")
                print(f"Site ID       : {siteid}")
                print(f"Duration      : {duration:.2f} seconds")
                print("-" * 60)

                continue  # 🔑 VERY IMPORTANT (skip crawler logic)

            # ====================================================
            # CRAWL / COMPARE MODE (UNCHANGED)
            # ====================================================

            frontier = Frontier()
            frontier.enqueue(start_url, None, 0)

            workers = []
            siteid_map = {siteid: siteid}

            for i in range(INITIAL_WORKERS):
                w = Worker(
                    frontier=frontier,
                    name=f"Worker-{i}",
                    custid=custid,
                    siteid_map=siteid_map,
                    job_id=job_id,
                    crawl_mode=CRAWL_MODE,
                    seed_url=start_url,
                    original_site_url=original_site_url,
                )
                w.start()
                workers.append(w)

            print(f"Started {len(workers)} workers.")

            frontier.queue.join()

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
            fail_crawl_job(job_id=job_id, err=str(e))
            print(f"ERROR: Crawl job {job_id} failed: {e}")
            import traceback
            traceback.print_exc()


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
