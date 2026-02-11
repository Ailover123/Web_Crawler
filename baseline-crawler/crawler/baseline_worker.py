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

    def __init__(self, *, custid, siteid, seed_url, target_urls=None, heartbeat_callback=None):
        self.custid = custid
        self.siteid = siteid
        self.seed_url = seed_url
        self.target_urls = target_urls
        self.heartbeat_callback = heartbeat_callback

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
                return "failed", f"Fetch failed for site={self.siteid} url={url}: {result.get('error')}", thread_name

            resp = result["response"]
            ct = resp.headers.get("Content-Type", "").lower()
            if "text/html" not in ct:
                return "skipped", f"Not HTML ({ct})", thread_name

            html_content = resp.text

            # ----------------------------------------------------
            # 🚀 SPA / React Detection & Escalation
            # ----------------------------------------------------
            from crawler.js_detect import needs_js_rendering
            if needs_js_rendering(html_content):
                try:
                    # logger.info(f"{thread_name} : [JS-RENDER] Escalating {url} (SPA detected)")
                    from crawler.js_renderer import render_js_sync
                    rendered_html, final_url = render_js_sync(fetch_url)
                    
                    if rendered_html and len(rendered_html) > len(html_content):
                        html_content = rendered_html
                        # logger.info(f"{thread_name} : [JS-RENDER] Success for {url} ({len(html_content)} bytes)")
                except Exception as e:
                    logger.warning(f"{thread_name} : [JS-RENDER] Failed for {url}: {e}")
            
            # ----------------------------------------------------
            # 🔗 Base Tag Injection (Fixes broken CSS/Images locally)
            # ----------------------------------------------------
            if "<base" not in html_content.lower():
                import re
                # Insert <base> immediately after <head>
                html_content = re.sub(
                    r"(<head[^>]*>)",
                    rf'\1<base href="{fetch_url}">',
                    html_content,
                    count=1,
                    flags=re.IGNORECASE
                )

            baseline_id, path, action = save_baseline(
                custid=self.custid,
                siteid=self.siteid,
                url=url,              # IMPORTANT: DB identity
                html=html_content,
                base_url=self.seed_url,
            )

            return action, f"id={baseline_id} url={url}", thread_name

        except Exception as e:
            return "failed", f"Exception for site={self.siteid} url={url}: {e}", thread_name

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
        failed_list = []

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

            future_to_url = {}

            for url in url_iter:
                f = executor.submit(self._process_url, url)
                future_to_url[f] = url
                # Invoke heartbeat while queuing (in case iterator is slow)
                if self.heartbeat_callback:
                    self.heartbeat_callback()

            if not future_to_url:
                logger.warning(
                    f"[BASELINE] No crawl_pages data found for site_id={self.siteid}. "
                    "Run CRAWL mode first."
                )
                return {
                    "created": 0,
                    "updated": 0,
                    "failed": 0,
                    "skipped": 0,
                    "failed_urls": []
                }

            for future in as_completed(future_to_url):
                url = future_to_url[future]
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
                        # details contains the error message
                        failed_list.append({"url": url, "error": details})
                        logger.error(
                            f"{worker_display} : [BASELINE] Failed {details}"
                        )

                    # Heartbeat every 100 URLs (prevents “silent hang”)
                    if processed % 100 == 0:
                        logger.info(
                            f"[BASELINE] Progress: processed={processed} "
                            f"created={created} updated={updated} failed={failed}"
                        )

                    # Invoke heartbeat to keep watchdog happy
                    if self.heartbeat_callback:
                        self.heartbeat_callback()

                except Exception as e:
                    failed += 1
                    failed_list.append({"url": url, "error": str(e)})
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
            "failed_urls": failed_list
        }
