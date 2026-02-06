#!/usr/bin/env python3
"""
Entry point for the web crawler and baseline generator.
"""

from pathlib import Path
from dotenv import load_dotenv
env_path = Path(__file__).resolve().parents[1] / '.env'
load_dotenv(dotenv_path=env_path)

import argparse
import logging
import threading
import time
import uuid
import os
import requests
import urllib3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from crawler.storage.mysql import fetch_site_info_by_baseline_id, site_has_baselines
from crawler.baseline_worker import BaselineWorker
from crawler.worker import SKIP_REPORT
from crawler.logger import logger
from crawler.config import (
    MIN_WORKERS,
    MAX_WORKERS,
    MAX_PARALLEL_SITES,
)
from crawler.worker import SKIP_LOCK
from crawler.throttle import (
    should_scale_down, 
    reset_scale_down, 
    set_pause
)

CRAWL_MODE = os.getenv("CRAWL_MODE", "CRAWL").upper()
assert CRAWL_MODE in ("BASELINE", "CRAWL", "COMPARE")

# Global crawl counters (Session Summary)
GLOBAL_SUCCESS = 0
GLOBAL_429_ERRORS = 0
GLOBAL_OTHER_ERRORS = 0
GLOBAL_TOTAL_URLS = 0
GLOBAL_START_TIME = time.time()
LAST_ACTIVITY_TIME = time.time()
GLOBAL_LOCK = threading.Lock()
WATCHDOG_TIMEOUT = 900  # 15 minutes hard timeout for inactivity

def watchdog_thread():
    """Kills the process if no activity is detected for WATCHDOG_TIMEOUT seconds."""
    while True:
        time.sleep(60)
        elapsed = time.time() - LAST_ACTIVITY_TIME
        if elapsed > WATCHDOG_TIMEOUT:
            logger.critical(f"FATAL: Watchdog timer expired! No activity for {elapsed:.0f}s. Force killing process.")
            os._exit(1)  # Force kill, no cleanup

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
    global GLOBAL_TOTAL_URLS, GLOBAL_SUCCESS, GLOBAL_429_ERRORS, GLOBAL_OTHER_ERRORS, LAST_ACTIVITY_TIME
    siteid = site["siteid"]
    custid = site["custid"]

    # 🔒 EXACT value from sites table (DB identity)
    original_site_url = site["url"].strip()

    # 🔑 Resolve seed FIRST, normalize AFTER
    from crawler.url_utils import canonicalize_seed, force_www_url

    resolved_seed = resolve_seed_url(original_site_url)
    
    # Respect the resolved seed exactly (don't force www)
    start_url = canonicalize_seed(resolved_seed)

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
            
            # Since BaselineWorker currently hardcodes max_workers=5
            worker_count = MAX_WORKERS
            logger.info(f"Worker-X : started (BASELINE) x{worker_count}")
            logger.info(f"Started {worker_count} workers.")

            stats = BaselineWorker(
                custid=custid,
                siteid=siteid,
                seed_url=start_url,
                target_urls=target_urls,
            ).run()

            # 🛡️ Safety: Ensure stats is always a dictionary
            if stats is None:
                stats = {"created": 0, "updated": 0, "failed": 0}

            # Calculate actual baseline counts
            baseline_count = stats.get('created', 0) + stats.get('updated', 0)
            
            duration = time.time() - start_time
            complete_crawl_job(job_id=job_id, pages_crawled=baseline_count)

            # Update global counters
            with threading.Lock():
                GLOBAL_TOTAL_URLS += baseline_count

            job_logger.info("-" * 60)
            job_logger.info("BASELINE GENERATION COMPLETED")
            job_logger.info(f"Site URL          : {original_site_url}")
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
        if CRAWL_MODE == "COMPARE":
             # 🛡️ High-level check: Verify baselines exist in DB and FS
             has_db_data = site_has_baselines(siteid)
             
             baseline_dir = Path("baselines") / str(custid) / str(siteid)
             has_fs_files = baseline_dir.exists() and any(baseline_dir.glob("*.html"))
             
             if has_db_data and not has_fs_files:
                  job_logger.error("files not present to compare")
                  job_logger.info(f"Summary: Site {siteid} has baseline records in DB but the 'baselines' directory is missing or empty. Please run baseline mode first.")
                  return
             
             if not has_db_data and has_fs_files:
                  job_logger.error("no data inside db to compare")
                  job_logger.info(f"Summary: Site {siteid} has files in 'baselines' directory but no records in DB. Please run baseline mode first.")
                  return
             
             if not has_db_data and not has_fs_files:
                  job_logger.warning(f"Site {siteid} has no baselines to compare against. Please run baseline mode first.")
                  return

        # 🛡️ Seed Skip Check
        from crawler.worker import classify_skip
        skip_reason = classify_skip(start_url)
        if skip_reason:
             job_logger.warning(f"Seed URL {start_url} skipped by rule: {skip_reason}")
             return

        frontier = Frontier()
        
        if target_urls:
             # Targeting specific pages only - do not seed the whole site
             for t_url in target_urls:
                 frontier.enqueue(t_url, None, 0, preference_url=original_site_url)
             job_logger.info(f"Targeting {len(target_urls)} specific URL(s). Site walking disabled.")
        else:
             # Default: Seed with start_url to crawl entire site
             frontier.enqueue(start_url, None, 0, preference_url=original_site_url)

        siteid_map = {siteid: siteid}
        workers = []
        try:
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
                    target_urls=target_urls,
                )
                w.start()
                workers.append(w)

            job_logger.info(f"Started {len(workers)} workers for site {siteid}.")
            
            # --- DYNAMIC SCALING LOOP ---
            while True:
                time.sleep(5)
                
                qsize = frontier.queue.qsize()
                unfinished = frontier.queue.unfinished_tasks
                
                if qsize == 0 and unfinished == 0:
                    break
                
                # --- ADAPTIVE SCALE DOWN ON 429 ---
                if should_scale_down(siteid) and len(workers) > MIN_WORKERS:
                    num_to_stop = len(workers) - MIN_WORKERS
                    job_logger.warning(f"429 hit! Scaling down {num_to_stop} workers to reach MIN_WORKERS ({MIN_WORKERS})")
                    for _ in range(num_to_stop):
                        if len(workers) > MIN_WORKERS:
                            w = workers.pop()
                            w.stop()
                            # join omitted to avoid blocking the main loop too long, 
                            # the finally block handles it anyway
                    reset_scale_down(siteid)
                
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
                            target_urls=target_urls,
                        )
                        w.start()
                        workers.append(w)
                    job_logger.info(f"Dynamically scaled up to {len(workers)} workers (Queue: {qsize})")

                if unfinished == 0:
                    break
                
                # HEARTBEAT: Keep watchdog happy while we are working
                LAST_ACTIVITY_TIME = time.time()
                    
        finally:
            # 🛑 CRITICAL: Always signal workers to stop and wait for them
            job_logger.info(f"Stopping workers for site {siteid}...")
            for w in workers:
                w.stop()
            
            # Wait with a "Hang Alert" loop
            start_join = time.time()
            for w in workers:
                if w.is_alive():
                    # Wait in small chunks to allow logging if it takes too long
                    while w.is_alive():
                        w.join(timeout=2)
                        
                        # HEARTBEAT during cleanup
                        LAST_ACTIVITY_TIME = time.time()

                        elapsed = time.time() - start_join
                        if elapsed > 15:
                             job_logger.warning(f"Thread {w.name} is taking exceptionally long to terminate ({elapsed:.1f}s). System may be hanging.")
                             break # Don't block forever if it's truly stuck
            
            # Reset the Global Site Pause so fresh runs aren't blocked
            set_pause(siteid, 0)
            reset_scale_down(siteid)
            
            job_logger.info(f"All workers for site {siteid} stopped.")

        duration = time.time() - start_time
        stats = frontier.get_stats()
        complete_crawl_job(job_id=job_id, pages_crawled=stats["visited_count"])

        total_saved = sum(getattr(w, 'saved_count', 0) for w in workers)
        total_db_existed = sum(getattr(w, 'existed_count', 0) for w in workers)
        total_failed = sum(getattr(w, 'failed_count', 0) for w in workers)
        total_policy_skipped = sum(getattr(w, 'policy_skipped_count', 0) for w in workers)
        total_frontier_skips = sum(getattr(w, 'frontier_duplicate_count', 0) for w in workers)
        total_skipped_rules = sum(data.get("count", 0) if isinstance(data, dict) else 0 for data in site_skip_report.values())
        
        total_duplicates = total_db_existed + total_frontier_skips
        total_visited = total_saved + total_duplicates + total_skipped_rules + total_failed + total_policy_skipped

        job_logger.info("-" * 60)
        job_logger.info(f"CRAWL COMPLETED FOR SITE {siteid}")
        
        display_crawled = total_saved
        if CRAWL_MODE == "COMPARE":
            display_crawled = total_saved + total_db_existed
            
        job_logger.info(f"Total URLs Crawled: {display_crawled}")
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

        with GLOBAL_LOCK:
            if CRAWL_MODE == "COMPARE":
                GLOBAL_TOTAL_URLS += (total_saved + total_db_existed)
            else:
                GLOBAL_TOTAL_URLS += total_saved
            GLOBAL_SUCCESS += total_saved
            GLOBAL_429_ERRORS += sum(getattr(w, 'failed_429_count', 0) for w in workers)
            GLOBAL_OTHER_ERRORS += total_failed

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


def main():
    # ... (existing parser args code) ...
    parser = argparse.ArgumentParser(description="Web Crawler / Baseline Tool")
    parser.add_argument("--siteid", type=int, nargs='+', help="Crawl one or more specific Site IDs")
    parser.add_argument("--baseline_id", "--baselineid", "--baseline-id", type=str, dest="baseline_id", help="Run only for this specific BASELINE ID")
    parser.add_argument("--custid", type=int, nargs='+', help="Crawl all sites for one or more specific Customer IDs")
    parser.add_argument("--mode", type=str, choices=["CRAWL", "BASELINE", "COMPARE"], help="Override CRAWL_MODE (CRAWL, BASELINE, COMPARE)")
    parser.add_argument("--parallel", action="store_true", help="Crawl multiple sites in parallel")
    parser.add_argument("--max_parallel_sites", type=int, help="Override MAX_PARALLEL_SITES limit")
    parser.add_argument("--log", action="store_true", help="Enable file logging for each job")

    args = parser.parse_args()

    # Override CRAWL_MODE if provided via CLI
    global CRAWL_MODE
    if args.mode:
        CRAWL_MODE = args.mode.upper()
        logger.info(f"CRAWL_MODE overridden by CLI: {CRAWL_MODE}")

    # Start Watchdog
    t = threading.Thread(target=watchdog_thread, daemon=True)
    t.start()
    logger.info(f"Watchdog active: Force kill if inactivity > {WATCHDOG_TIMEOUT}s")

    file_handler = None # Initialize to avoid NameError in finally block

    # ... (existing db check and site fetching) ...

    # ---------------- DB CHECK ----------------
    if not check_db_health():
        logger.error("MySQL health check failed.")
        return

    logger.info("MySQL health check passed.")

    target_urls = None
    
    if args.baseline_id:
        # Priority: Fetch specific baseline info directly (bypassing enabled check if needed)
        info = fetch_site_info_by_baseline_id(args.baseline_id)
        if not info:
             logger.error(f"Baseline ID '{args.baseline_id}' not found in defacement_sites.")
             return
        
        target_siteid = info["siteid"]
        target_urls = [info["url"]]
        
        # Construct site object manually from the DB info
        sites = [{
            "siteid": info["siteid"], 
            "custid": info["custid"], 
            "url": info["url"]
        }]
        logger.info(f"--> Targeting specific Baseline ID: {args.baseline_id} (Site {target_siteid}, Cust {info['custid']})")
    
    else:
        # Standard Mode: Fetch all enabled sites
        sites = fetch_enabled_sites()
        if not sites:
            logger.info("No enabled sites found.")
            return

        if args.siteid:
            sites = [s for s in sites if s["siteid"] in args.siteid]
        if args.custid:
            sites = [s for s in sites if s["custid"] in args.custid]

    if not sites:
        logger.info("No sites matched the specified filters.")
        return

    logger.info(f"Found {len(sites)} enabled site(s) to process.")

    # LOGGING SETUP
    if args.log:
        today_dir = datetime.now().strftime('%Y-%m-%d')
        log_dir = os.path.join("logs", today_dir)
        os.makedirs(log_dir, exist_ok=True)
        
        log_filename = f"{CRAWL_MODE}_{datetime.now().strftime('%H%M%S')}.log"
        log_path = os.path.join(log_dir, log_filename)
        
        # Manually attach FileHandler since setup_logger returns early if handlers exist
        import logging
        from crawler.logger import CompanyFormatter
        
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(CompanyFormatter())
        logger.addHandler(file_handler)
        
        logger.info(f"--- Session started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
        logger.info(f"Log file: {log_path}")

    # ---------------- EXECUTION ----------------
    try:
        if args.parallel:
            max_parallel_sites = args.max_parallel_sites or MAX_PARALLEL_SITES
            logger.info(f"Parallel mode enabled (max {max_parallel_sites} sites at once).")

            # Batch processing to prevent resource exhaustion (leaks, open files, etc.)
            BATCH_SIZE = 20
            
            def chunked_sites(iterable, n):
                for i in range(0, len(iterable), n):
                    yield iterable[i:i + n]

            site_batches = list(chunked_sites(sites, BATCH_SIZE))
            total_batches = len(site_batches)

            for i, batch in enumerate(site_batches, 1):
                logger.info(f"Processing Batch {i}/{total_batches} ({len(batch)} sites)...")
                
                # Create a FRESH executor for each batch to ensure threads are cleaned up
                with ThreadPoolExecutor(max_workers=max_parallel_sites) as executor:
                    future_to_site = {executor.submit(crawl_site, s, args, target_urls): s for s in batch}
                    
                    site_timeout = int(os.getenv("SITE_PROCESS_TIMEOUT", 1800))
                    
                    for future in as_completed(future_to_site):
                        site = future_to_site[future]
                        try:
                            future.result(timeout=site_timeout)
                            
                            # Reset watchdog
                            global LAST_ACTIVITY_TIME
                            LAST_ACTIVITY_TIME = time.time()
                            
                        except Exception as e:
                            logger.error(f"Site {site.get('siteid')} task failed or timed out ({site_timeout}s): {e}")

                logger.info(f"Batch {i}/{total_batches} completed.")
                # Small cool-down between batches
                time.sleep(2)

        else:
            for site in sites:
                crawl_site(site, args, target_urls)
    finally:
        if file_handler:
            total_duration = time.time() - GLOBAL_START_TIME
            logger.info("\n" + "=" * 60)
            logger.info("           GLOBAL SESSION SUMMARY")
            logger.info("=" * 60)
            logger.info(f" Total Sites Processed : {len(sites)}")
            logger.info(f" Total URLs Audited    : {GLOBAL_TOTAL_URLS}")
            logger.info(f" Successfully Saved    : {GLOBAL_SUCCESS}")
            logger.info(f" Rate Limit (429)      : {GLOBAL_429_ERRORS}")
            logger.info(f" Other Failures        : {GLOBAL_OTHER_ERRORS - GLOBAL_429_ERRORS}")
            logger.info("-" * 60)
            logger.info(f" Total Session Time    : {total_duration:.2f} seconds")
            if total_duration > 0:
                logger.info(f" Overall Throughput    : {GLOBAL_TOTAL_URLS / total_duration:.2f} URLs/sec")
            logger.info("=" * 60 + "\n")
            
            logger.info(f"--- Session ended: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
            logger.removeHandler(file_handler)
            file_handler.close()

    logger.info("All site crawls processed successfully.")

    # AUTOMATED REPORT GENERATION
    try:
        from report_generator import generate_report
        generate_report()
    except Exception as e:
        logger.error(f"Failed to generate automated report: {e}")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    import sys
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
    
    sys.exit(0)
