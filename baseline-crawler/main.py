#!/usr/bin/env python3
"""
Entry point for the web crawler and baseline generator.
"""

from dotenv import load_dotenv
load_dotenv()

import argparse
import logging
import threading
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
from crawler.storage.mysql import fetch_site_info_by_baseline_id
from crawler.baseline_worker import BaselineWorker
from crawler.worker import SKIP_REPORT
from crawler.logger import logger
from crawler.config import (
    MIN_WORKERS,
    MAX_WORKERS,
    MAX_PARALLEL_SITES,
)
from crawler.worker import SKIP_LOCK

CRAWL_MODE = os.getenv("CRAWL_MODE", "CRAWL").upper()
assert CRAWL_MODE in ("BASELINE", "CRAWL", "COMPARE")

# Global crawl counters (Session Summary)
GLOBAL_TOTAL_URLS = 0
GLOBAL_START_TIME = time.time()

# ============================================================
# SEED URL RESOLUTION
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
                timeout=12,
                allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"},
                verify=False,
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
# PER-SITE CRAWL LOGIC
# ============================================================

def crawl_site(site, args, target_urls=None):
    """
    Crawls a single site. This function can be called sequentially or in parallel.
    """
    siteid = site["siteid"]
    custid = site["custid"]

    # 🔒 EXACT value from sites table (DB identity)
    original_site_url = site["url"].strip()

    # 🔑 Resolve seed FIRST, normalize AFTER
    from crawler.url_utils import canonicalize_seed, force_www_url

    resolved_seed = resolve_seed_url(original_site_url)
    
    # Force www for fetch/start
    start_url = force_www_url(canonicalize_seed(resolved_seed))

    job_id = str(uuid.uuid4())
    
    # Site-local metrics for thread-safety in parallel mode
    from collections import defaultdict
    site_skip_report = defaultdict(lambda: {"count": 0, "urls": []})
    site_skip_lock = threading.Lock()

    job_logger = logger

    job_logger.info("=" * 60)
    job_logger.info(f"Starting job {job_id} ({CRAWL_MODE})")
    job_logger.info(f"Customer ID    : {custid}")
    job_logger.info(f"Site ID        : {siteid}")
    job_logger.info(f"Registered URL : {original_site_url}")
    job_logger.info(f"Starting URL   : {start_url}")
    job_logger.info("=" * 60)

    try:
        insert_crawl_job(
            job_id=job_id,
            custid=custid,
            siteid=siteid,
            start_url=start_url,
        )

        start_time = time.time()

        # ====================================================
        # BASELINE MODE (REFETCH FROM DB)
        # ====================================================
        if CRAWL_MODE == "BASELINE":
            job_logger.info(f"[MODE] BASELINE (offline refetch from DB for siteid={siteid})")
            
            # Since BaselineWorker currently hardcodes max_workers=10
            worker_count = 10
            for i in range(worker_count):
                job_logger.info(f"Worker-{i} : started (BASELINE)")
            job_logger.info(f"Started {worker_count} workers.")

            stats = BaselineWorker(
                custid=custid,
                siteid=siteid,
                seed_url=start_url,
                target_urls=target_urls,
            ).run()

            # 🛡️ Safety: Ensure stats is always a dictionary
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
        frontier.enqueue(start_url, None, 0, preference_url=original_site_url)

        workers = []
        siteid_map = {siteid: siteid}

        for i in range(MIN_WORKERS):
            w = Worker(
                frontier=frontier,
                name=f"Worker-{siteid}-{i}",
                custid=custid,
                siteid_map=siteid_map,
                job_id=job_id,
                crawl_mode=CRAWL_MODE,
                seed_url=start_url, 
                original_site_url=original_site_url,
                skip_report=site_skip_report,
                skip_lock=site_skip_lock,
            )
            w.start()
            workers.append(w)

        job_logger.info(f"Started {len(workers)} workers for site {siteid}.")
        
        # --- DYNAMIC SCALING LOOP ---
        try:
            while True:
                # Wait for queue to process
                time.sleep(5)
                
                qsize = frontier.queue.qsize()
                unfinished = frontier.queue.unfinished_tasks
                
                # Scale up if queue is pressurized
                if qsize > 100 and len(workers) < MAX_WORKERS:
                    scale_count = min(5, MAX_WORKERS - len(workers))
                    for i in range(scale_count):
                        w = Worker(
                            frontier=frontier,
                            name=f"Worker-{siteid}-{len(workers)}",
                            custid=custid,
                            siteid_map=siteid_map,
                            job_id=job_id,
                            crawl_mode=CRAWL_MODE,
                            seed_url=start_url, 
                            original_site_url=original_site_url,
                            skip_report=site_skip_report,
                            skip_lock=site_skip_lock,
                        )
                        w.start()
                        workers.append(w)
                    job_logger.info(f"Dynamically scaled up to {len(workers)} workers (Queue: {qsize})")

                if unfinished == 0:
                    break
        except KeyboardInterrupt:
            job_logger.warning("Scaling loop interrupted.")

        for w in workers: w.stop()
        for w in workers: w.join()

        duration = time.time() - start_time
        stats = frontier.get_stats()
        complete_crawl_job(job_id=job_id, pages_crawled=stats["visited_count"])

        total_saved = sum(getattr(w, 'saved_count', 0) for w in workers)
        total_db_existed = sum(getattr(w, 'existed_count', 0) for w in workers)
        total_failed = sum(getattr(w, 'failed_count', 0) for w in workers)
        total_policy_skipped = sum(getattr(w, 'policy_skipped_count', 0) for w in workers)
        total_frontier_skips = sum(getattr(w, 'frontier_duplicate_count', 0) for w in workers)
        total_skipped_rules = sum(data.get("count", 0) if isinstance(data, dict) else 0 for data in site_skip_report.values())
        
        # Total Duplicates = DB Existed + Frontier Skips
        total_duplicates = total_db_existed + total_frontier_skips
        
        # Total Visited = Crawled + Duplicates + Skipped + Failed + Policy Skips
        total_visited = total_saved + total_duplicates + total_skipped_rules + total_failed + total_policy_skipped

        job_logger.info("-" * 60)
        job_logger.info(f"CRAWL COMPLETED FOR SITE {siteid}")
        job_logger.info(f"Total URLs Crawled: {total_saved}")
        job_logger.info(f"Total URLs Visited: {total_visited}")
        job_logger.info(f"Duplicates Skipped: {total_duplicates}")
        job_logger.info(f" (Frontier Skips) : {total_frontier_skips}")
        job_logger.info(f" (DB Existed)     : {total_db_existed}")
        job_logger.info(f"Policy Skipped    : {total_policy_skipped}")
        job_logger.info(f"URLs Skipped      : {total_skipped_rules}")
        job_logger.info(f"URLs Failed       : {total_failed}")
        job_logger.info(f"Crawl duration    : {duration:.2f} seconds")
        job_logger.info(f"Workers used      : {len(workers)}")
        job_logger.info("-" * 60)

        # Update global counters
        global GLOBAL_TOTAL_URLS
        with threading.Lock():
            GLOBAL_TOTAL_URLS += total_saved

        # Aggregation logic
        with site_skip_lock:
            for skip_type, data in site_skip_report.items():
                with SKIP_LOCK:
                    SKIP_REPORT[skip_type]["count"] += data["count"]
                    SKIP_REPORT[skip_type]["urls"].extend(data["urls"][:5])

    except Exception as e:
        fail_crawl_job(job_id, str(e))
        job_logger.error(f"Crawl job {job_id} failed: {e}")
        import traceback
        traceback.print_exc()


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Web Crawler / Baseline Tool")
    parser.add_argument("--siteid", type=int, nargs='+', help="Crawl one or more specific Site IDs")
    parser.add_argument("--custid", type=int, nargs='+', help="Crawl all sites for one or more specific Customer IDs")
    parser.add_argument("--baseline_id", type=str, help="Run only for this specific BASELINE ID")
    parser.add_argument("--parallel", action="store_true", help="Crawl multiple sites in parallel")
    parser.add_argument("--max_parallel_sites", type=int, help="Override MAX_PARALLEL_SITES limit")
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

    # ---------------- FILTER SITES ----------------
    target_urls = None
    
    if args.baseline_id:
        info = fetch_site_info_by_baseline_id(args.baseline_id)
        if not info:
             logger.error(f"Baseline ID '{args.baseline_id}' not found in defacement_sites.")
             return
        target_siteid = info["siteid"]
        target_urls = [info["url"]]
        sites = [s for s in sites if s["siteid"] == target_siteid]
        logger.info(f"--> Targeting specific Baseline ID: {args.baseline_id} (Site {target_siteid})")
    else:
        if args.siteid:
            sites = [s for s in sites if s["siteid"] in args.siteid]
        if args.custid:
            sites = [s for s in sites if s["custid"] in args.custid]

    if not sites:
        logger.info("No sites matched the specified filters.")
        return

    logger.info(f"Found {len(sites)} enabled site(s) to process.")

    # LOGGING SETUP
    file_handler = None
    if args.log:
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        log_filename = f"{CRAWL_MODE}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log"
        log_path = os.path.join(log_dir, log_filename)
        
        file_handler = logging.FileHandler(log_path)
        from crawler.logger import CompanyFormatter
        file_handler.setFormatter(CompanyFormatter())
        logger.addHandler(file_handler)
        logger.info(f"--- Session started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

    # ---------------- EXECUTION ----------------
    try:
        if args.parallel:
            max_parallel_sites = args.max_parallel_sites or MAX_PARALLEL_SITES
            logger.info(f"Parallel mode enabled (max {max_parallel_sites} sites at once).")
            with ThreadPoolExecutor(max_workers=max_parallel_sites) as executor:
                list(executor.map(lambda s: crawl_site(s, args, target_urls), sites))
        else:
            for site in sites:
                crawl_site(site, args, target_urls)
    finally:
        if file_handler:
            total_duration = time.time() - GLOBAL_START_TIME
            logger.info("=" * 60)
            logger.info("GLOBAL SESSION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Total Sites Processed : {len(sites)}")
            logger.info(f"Total Global URLs     : {GLOBAL_TOTAL_URLS}")
            logger.info(f"Total Session Time    : {total_duration:.2f} seconds")
            if total_duration > 0:
                logger.info(f"Overall Throughput    : {GLOBAL_TOTAL_URLS / total_duration:.2f} URLs/sec")
            logger.info("=" * 60)
            
            logger.info(f"--- Session ended: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
            logger.removeHandler(file_handler)
            file_handler.close()

    logger.info("All site crawls processed successfully.")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    main()

    if SKIP_REPORT:
        print("\n" + "=" * 60)
        print("GLOBAL SKIPPED URL REPORT")
        print("=" * 60)
        for skip_type, data in SKIP_REPORT.items():
            try:
                count = len(data.get("urls", []))
                urls = data.get("urls", [])
            except Exception:
                count = 0
                urls = []

            print(f"[{skip_type}] {count} URLs skipped")
            for u in urls[:10]: # Limit print for brevity
                print(f"  - {u}")
        print("=" * 60)
