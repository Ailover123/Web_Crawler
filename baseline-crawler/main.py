#!/usr/bin/env python3
"""
Entry point for the web crawler.
Uses queue.join() for deterministic crawl completion.
"""

import site
from dotenv import load_dotenv
load_dotenv()

import argparse
import logging
import time
import uuid
import os
import requests
import urllib3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
                verify=False,
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

def crawl_site(site, args):
    """
    Crawls a single site. This function can be called sequentially or in parallel.
    """
    siteid = site["siteid"]
    custid = site["custid"]

    # 🔑 Resolve seed FIRST, normalize AFTER
    from crawler.url_utils import canonicalize_seed, force_www_url

    resolved_seed = resolve_seed_url(site["url"])
    
    # Force www for fetch/start
    start_url = force_www_url(canonicalize_seed(resolved_seed))

    job_id = str(uuid.uuid4())
    
    # Setup job logging if requested
    job_logger = logger
    file_handler = None
    if args.log:
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"crawl_{siteid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        job_logger.addHandler(file_handler)

    job_logger.info("=" * 60)
    job_logger.info(f"Starting crawl job {job_id}")
    job_logger.info(f"Customer ID : {custid}")
    job_logger.info(f"Site ID     : {siteid}")
    job_logger.info(f"Seed URL    : {start_url}")
    job_logger.info("=" * 60)

    try:
        insert_crawl_job(
            job_id=job_id,
            custid=custid,
            siteid=siteid,
            start_url=start_url,
        )

        # ====================================================
        # BASELINE MODE (REFETCH FROM DB)
        # ====================================================
        if CRAWL_MODE == "BASELINE":
            from crawler.baseline_worker import BaselineWorker
            job_logger.info(f"[MODE] BASELINE (offline refetch from DB for siteid={siteid})")

            start_time = time.time()
            stats = BaselineWorker(
                custid=custid,
                siteid=siteid,
                seed_url=start_url,
            ).run()

            # 🛡️ Safety: Ensure stats is always a dictionary to avoid 'NoneType' errors
            if stats is None:
                stats = {"created": 0, "updated": 0, "failed": 0}

            duration = time.time() - start_time
            complete_crawl_job(job_id=job_id, pages_crawled=0)

            job_logger.info("-" * 60)
            job_logger.info("BASELINE GENERATION COMPLETED")
            job_logger.info("-" * 60)
            job_logger.info(f"Baselines Created : {stats.get('created', 0)}")
            job_logger.info(f"Baselines Updated : {stats.get('updated', 0)}")
            job_logger.info(f"Baselines Failed  : {stats.get('failed', 0)}")
            job_logger.info(f"Duration          : {duration:.2f} seconds")
            job_logger.info("-" * 60)
            return

        # ====================================================
        # CRAWL / COMPARE MODE (LIVE DISCOVERY)
        # ====================================================
        frontier = Frontier()
        frontier.enqueue(start_url, None, 0, preference_url=site["url"])

        workers = []
        start_time = time.time()
        siteid_map = {siteid: siteid}

        for i in range(INITIAL_WORKERS):
            w = Worker(
                frontier=frontier,
                name=f"Worker-{siteid}-{i}",
                custid=custid,
                siteid_map=siteid_map,
                job_id=job_id,
                crawl_mode=CRAWL_MODE,
                seed_url=site["url"],
            )
            w.start()
            workers.append(w)

        job_logger.info(f"Started {len(workers)} workers for site {siteid}.")
        frontier.queue.join()

        for w in workers: w.stop()
        for w in workers: w.join()

        duration = time.time() - start_time
        stats = frontier.get_stats()
        complete_crawl_job(job_id=job_id, pages_crawled=stats["visited_count"])

        total_saved = sum(w.saved_count for w in workers)
        job_logger.info("-" * 60)
        job_logger.info(f"CRAWL COMPLETED FOR SITE {siteid}")
        job_logger.info(f"Total URLs Crawled: {total_saved}")
        job_logger.info(f"Crawl duration    : {duration:.2f} seconds")
        job_logger.info("-" * 60)

    except Exception as e:
        fail_crawl_job(job_id, str(e))
        job_logger.error(f"Crawl job {job_id} failed: {e}")
    finally:
        if file_handler:
            job_logger.removeHandler(file_handler)
            file_handler.close()

def main():
    parser = argparse.ArgumentParser(description="Web Crawler Entry Point")
    parser.add_argument("--siteid", type=int, nargs='+', help="Crawl one or more specific Site IDs")
    parser.add_argument("--custid", type=int, nargs='+', help="Crawl all sites for one or more specific Customer IDs")
    parser.add_argument("--parallel", action="store_true", help="Crawl multiple sites in parallel")
    parser.add_argument("--log", action="store_true", help="Enable file logging for each job")
    args = parser.parse_args()

    # ---------------- DB CHECK ----------------
    if not check_db_health():
        logger.error("MySQL health check failed.")
        return

    logger.info("MySQL health check passed.")

    sites = fetch_enabled_sites()
    if not sites:
        logger.info("No enabled sites found.")
        return

    # Filter sites based on arguments
    if args.siteid:
        sites = [s for s in sites if s["siteid"] in args.siteid]
    if args.custid:
        sites = [s for s in sites if s["custid"] in args.custid]

    if not sites:
        logger.info("No sites matched the specified filters.")
        return

    logger.info(f"Processing {len(sites)} site(s).")

    # ---------------- EXECUTION ----------------
    if args.parallel:
        max_parallel_sites = int(os.getenv("MAX_PARALLEL_SITES", 3))
        logger.info(f"Parallel mode enabled (max {max_parallel_sites} sites at once).")
        with ThreadPoolExecutor(max_workers=max_parallel_sites) as executor:
            executor.map(lambda s: crawl_site(s, args), sites)
    else:
        for site in sites:
            crawl_site(site, args)

    logger.info("All site crawls processed successfully.")


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
