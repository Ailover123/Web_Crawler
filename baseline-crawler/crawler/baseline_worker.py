# crawler/baseline_worker.py

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from crawler.fetcher import fetch
from crawler.storage.crawl_reader import iter_crawl_urls
from crawler.storage.baseline_store import save_baseline
from crawler.normalizer import normalize_url
from crawler.url_utils import force_www_url
from crawler.logger import logger


class BaselineWorker:
    """
    BaselineWorker:
    - Uses ONLY URLs from crawl_pages
    - Streams URLs (no big list in memory)
    - Safely parallelizes fetch + baseline update
    """

    def __init__(self, *, custid, siteid, seed_url, target_urls=None):
        self.custid = custid
        self.siteid = siteid
        self.seed_url = seed_url
        self.target_urls = target_urls

        # Use MAX_WORKERS from config, but ensure we don't exceed reasonable limits
        # relative to the DB pool size if running mainly in parallel.
        from crawler.config import MAX_WORKERS
        self.max_workers = MAX_WORKERS

    # ------------------------------------------------------------
    # Worker function (runs in thread pool)
    # ------------------------------------------------------------
    def _process_url(self, url: str):
        thread_name = threading.current_thread().name

        try:
            # Normalize + force www (network fetch only)
            fetch_url = normalize_url(url, preference_url=self.seed_url)
            fetch_url = force_www_url(fetch_url)

            result = fetch(fetch_url)
            if not result["success"]:
                return "failed", f"Fetch failed: {result.get('error')}", thread_name

            resp = result["response"]
            ct = resp.headers.get("Content-Type", "").lower()
            if "text/html" not in ct:
                return "skipped", f"Not HTML ({ct})", thread_name

            baseline_id, path, action = save_baseline(
                custid=self.custid,
                siteid=self.siteid,
                url=url,              # IMPORTANT: DB identity
                html=resp.text,
                base_url=self.seed_url,
            )

            return action, f"id={baseline_id} url={url}", thread_name

        except Exception as e:
            return "failed", f"Exception: {e}", thread_name

    # ------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------
    def run(self):
        logger.info(
            f"[BASELINE] Controlled baseline refetch started "
            f"for site_id={self.siteid}"
        )

        # --------------------------------------------------------
        # Determine input URL source
        # --------------------------------------------------------
        if self.target_urls is not None:
            logger.info(
                f"[BASELINE] Targeting {len(self.target_urls)} specific URL(s)."
            )
            url_iter = iter(self.target_urls)
        else:
            # STREAMING iterator (CRITICAL FIX)
            url_iter = iter_crawl_urls(siteid=self.siteid)

        # --------------------------------------------------------
        # Counters
        # --------------------------------------------------------
        created = 0
        updated = 0
        failed = 0
        skipped = 0
        processed = 0

        logger.info(
            f"[BASELINE] Parallel fetch with {self.max_workers} workers"
        )

        # --------------------------------------------------------
        # Thread pool
        # --------------------------------------------------------
        with ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="Worker"
        ) as executor:

            futures = []

            for url in url_iter:
                futures.append(executor.submit(self._process_url, url))

            if not futures:
                logger.warning(
                    f"[BASELINE] No crawl_pages data found for site_id={self.siteid}. "
                    "Run CRAWL mode first."
                )
                return {
                    "created": 0,
                    "updated": 0,
                    "failed": 0,
                    "skipped": 0,
                }

            for future in as_completed(futures):
                try:
                    action, details, thread_name = future.result()
                    processed += 1

                    # Normalize thread name to Worker-X
                    worker_display = thread_name
                    if "_" in thread_name:
                        try:
                            prefix, idx = thread_name.rsplit("_", 1)
                            if prefix.startswith("Worker"):
                                worker_display = f"Worker-{idx}"
                        except Exception:
                            pass

                    if action == "created":
                        created += 1
                        logger.info(
                            f"{worker_display} : [BASELINE] Created {details}"
                        )
                    elif action == "updated":
                        updated += 1
                        logger.info(
                            f"{worker_display} : [BASELINE] Updated {details}"
                        )
                    elif action == "skipped":
                        skipped += 1
                    else:
                        failed += 1
                        logger.error(
                            f"{worker_display} : [BASELINE] Failed {details}"
                        )

                    # Heartbeat every 100 URLs (prevents “silent hang”)
                    if processed % 100 == 0:
                        logger.info(
                            f"[BASELINE] Progress: processed={processed} "
                            f"created={created} updated={updated} failed={failed}"
                        )

                except Exception as e:
                    failed += 1
                    logger.error(f"[BASELINE] Worker exception: {e}")

        # --------------------------------------------------------
        # Final summary
        # --------------------------------------------------------
        logger.info(
            f"[BASELINE] Done | "
            f"created={created} "
            f"updated={updated} "
            f"failed={failed} "
            f"skipped={skipped}"
        )

        return {
            "created": created,
            "updated": updated,
            "failed": failed,
            "skipped": skipped,
        }
