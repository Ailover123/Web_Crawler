#!/usr/bin/env python3
"""
Entry point for the web crawler.
Uses queue.join() for deterministic crawl completion.
"""

import site
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

from crawler.logger import logger

from crawler.worker import BLOCK_REPORT
#from crawler.compare_engine import DEFACEMENT_REPORT

CRAWL_MODE = os.getenv("CRAWL_MODE", "CRAWL").upper()
assert CRAWL_MODE in ("BASELINE", "CRAWL", "COMPARE")

INITIAL_WORKERS = 5
MAX_WORKERS = 20
SCALE_THRESHOLD = 100


# ============================================================
# SEED URL RESOLUTION (CRITICAL FIX)
# ============================================================

def resolve_seed_url(raw_url: str) -> str:
    """
    Resolve the correct root URL for crawling.

    Tries:
      1) without trailing slash
      2) with trailing slash

    Locks the first variant that responds successfully.
    """
    raw = raw_url.strip()

    if raw.endswith("/"):
        candidates = [raw.rstrip("/"), raw]
    else:
        candidates = [raw, raw + "/"]

    for u in candidates:
        try:
            r = requests.get(
                u,
                timeout=12,
                allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"},
            )
            if r.status_code < 400:
                # lock final resolved URL
                return r.url
        except Exception:
            continue

    # fallback: ensure scheme is present
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    return raw


# ============================================================
# MAIN
# ============================================================

def main():
    # ---------------- DB CHECK ----------------
    if not check_db_health():
        logger.error("MySQL health check failed.")
        return

    logger.info("MySQL health check passed.")

    sites = fetch_enabled_sites()
    if not sites:
        logger.info("No enabled sites found.")
        return

    logger.info(f"Found {len(sites)} enabled site(s).")

    # ---------------- PER SITE ----------------
    for site in sites:
        siteid = site["siteid"]
        custid = site["custid"]

        # 🔑 Resolve seed FIRST, normalize AFTER
        from crawler.url_utils import canonicalize_seed

        resolved_seed = resolve_seed_url(site["url"])
        start_url = canonicalize_seed(resolved_seed)


        job_id = str(uuid.uuid4())
        
        # Reset global block report for this site
        from crawler.worker import BLOCK_REPORT
        BLOCK_REPORT.clear()

        logger.info("=" * 60)
        logger.info(f"Starting crawl job {job_id}")
        logger.info(f"Customer ID : {custid}")
        logger.info(f"Site ID     : {siteid}")
        logger.info(f"Seed URL    : {start_url}")
        logger.info("=" * 60)

        try:
            insert_crawl_job(
                job_id=job_id,
                custid=custid,
                siteid=siteid,
                start_url=start_url,
            )

            frontier = Frontier()
            frontier.enqueue(start_url, None, 0, preference_url=site["url"])

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
                    seed_url=site["url"], # 🔒 USE REGISTERED URL FOR NAMING PREFERENCE
                )
                w.start()
                workers.append(w)

            logger.info(f"Started {len(workers)} workers.")

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

            logger.info("-" * 60)
            logger.info("CRAWL COMPLETED")
            logger.info("-" * 60)
            logger.info(f"Job ID            : {job_id}")
            logger.info(f"Customer ID       : {custid}")
            logger.info(f"Site ID           : {siteid}")
            logger.info(f"Seed URL          : {start_url}")
            
            total_saved = sum(w.saved_count for w in workers)
            total_db_updates = sum(w.duplicate_count for w in workers)
            total_failed = sum(w.failed_count for w in workers)
            total_policy_skipped = sum(w.policy_skipped_count for w in workers)
            total_frontier_skips = sum(w.frontier_duplicate_count for w in workers)
            total_blocked = sum(data.get("count", 0) if isinstance(data, dict) else 0 for data in BLOCK_REPORT.values())
            
            # Total Duplicates = DB Updates + Frontier Skips
            total_duplicates = total_db_updates + total_frontier_skips
            
            # Total Visited = Crawled + Duplicates + Blocked + Failed + Policy Skips
            total_visited = total_saved + total_duplicates + total_blocked + total_failed + total_policy_skipped

            logger.info(f"Total URLs Crawled: {total_saved}")
            logger.info(f"Total URLs Visited: {total_visited}")
            logger.info(f"Duplicates Skipped: {total_duplicates}")
            logger.info(f" (Frontier Skips) : {total_frontier_skips}")
            logger.info(f" (DB Updates)     : {total_db_updates}")
            logger.info(f"Policy Skipped    : {total_policy_skipped}")
            logger.info(f"URLs Blocked      : {total_blocked}")
            logger.info(f"URLs Failed       : {total_failed}")
            logger.info(f"Crawl duration    : {duration:.2f} seconds")
            logger.info(f"Workers used      : {len(workers)}")
            logger.info("-" * 60)

        except Exception as e:
            fail_crawl_job(job_id, str(e))
            logger.error(f"Crawl job {job_id} failed: {e}")
            raise

    logger.info("All site crawls completed successfully.")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    main()

    if BLOCK_REPORT:
        logger.info("=" * 60)
        logger.info("BLOCKED URL REPORT")
        logger.info("=" * 60)
        for block_type, data in BLOCK_REPORT.items():
            # New format: dict with 'count' and 'urls'; keep backward compatibility
            if isinstance(data, dict) and "count" in data:
                count = data.get("count", 0)
                urls = data.get("urls", [])
            elif isinstance(data, int):
                count = data
                urls = []
            else:
                # fallback for list-like
                try:
                    count = len(data)
                except Exception:
                    count = 0
                urls = list(data) if hasattr(data, '__iter__') else []

            logger.info(f"[{block_type}] {count} URLs blocked")
            if urls:
                for u in urls:
                    logger.info(f"  - {u}")
        logger.info("=" * 60)
        logger.info("=" * 60)
