# crawler/baseline_worker.py

from crawler.fetcher import fetch
from crawler.storage.crawl_reader import iter_crawl_urls
from crawler.storage.baseline_store import save_baseline
from crawler.normalizer import normalize_url
from crawler.url_utils import force_www_url
from crawler.logger import logger


class BaselineWorker:
    def __init__(self, *, custid, siteid, seed_url, target_urls=None):
        self.custid = custid
        self.siteid = siteid
        self.seed_url = seed_url
        self.target_urls = target_urls

    def run(self):
        logger.info(f"[BASELINE] Controlled baseline refetch started for site_id={self.siteid}")

        if self.target_urls:
             logger.info(f"[BASELINE] Targeting {len(self.target_urls)} specific URL(s).")
             urls = self.target_urls
        else:
            urls = iter_crawl_urls(siteid=self.siteid)

        if not urls:
            logger.warning(
                f"[BASELINE] No crawl_pages data found for site_id={self.siteid}. "
                "Run CRAWL mode first."
            )
            return {"created": 0, "updated": 0, "failed": 0}

        created = 0
        updated = 0
        failed = 0

        # Parallel Execution for faster baseline collection
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        # Helper function for parallel execution
        def process_url(url):
            import threading
            worker_name = threading.current_thread().name
            # Simply renaming threads to look like "Worker-X" is tricky in a pool without custom init,
            # but ThreadPoolExecutor threads are usually named "ThreadPoolExecutor-0_0".
            # We can just use the thread name as is, or map it.
            
            try:
                fetch_url = normalize_url(
                    url,
                    preference_url=self.seed_url
                )
                
                # 🔒 Force www prefix for refetching
                fetch_url = force_www_url(fetch_url)

                result = fetch(fetch_url)
                if not result["success"]:
                    logger.error(f"[BASELINE] Fetch failed for {fetch_url}: {result.get('error')}")
                    return "failed", f"Fetch failed: {result.get('error')}", worker_name

                resp = result["response"]
                ct = resp.headers.get("Content-Type", "").lower()
                if "text/html" not in ct:
                    return "skipped", "Not HTML", worker_name

                baseline_id, path, action = save_baseline(
                    custid=self.custid,
                    siteid=self.siteid,
                    url=url,
                    html=resp.text,
                    base_url=self.seed_url,
                )
                
                return action, f"{baseline_id} for {url}", worker_name

            except Exception as e:
                logger.error(f"[BASELINE] Error processing {url}: {e}")
                return "failed", f"Error: {e}", worker_name

        logger.info(f"[BASELINE] Parallel fetch with 10 workers for {len(urls)} URLs...")

        # Use thread_name_prefix="Worker" to get names like "Worker_0", "Worker_1" etc.
        # Note: ThreadPoolExecutor appends "_X" (index).
        with ThreadPoolExecutor(max_workers=10, thread_name_prefix="Worker") as executor:
            future_to_url = {executor.submit(process_url, url): url for url in urls}
            
            for future in as_completed(future_to_url):
                try:
                    action, details, thread_name = future.result()
                    
                    # Clean up thread name to match "Worker-X" format
                    # ThreadPoolExecutor names are typically "Worker_0", "Worker_1"
                    # We want "Worker-0", "Worker-1"
                    if "_" in thread_name:
                         try:
                             prefix, idx = thread_name.rsplit("_", 1)
                             if "Worker" in prefix:
                                 worker_display_name = f"Worker-{idx}"
                             else:
                                 worker_display_name = thread_name
                         except:
                             worker_display_name = thread_name
                    else:
                        worker_display_name = thread_name

                    if action == "created":
                        logger.info(f"{worker_display_name} : [BASELINE] Created baseline {details}")
                        created += 1
                    elif action == "updated":
                        logger.info(f"{worker_display_name} : [BASELINE] Updated baseline {details}")
                        updated += 1
                    elif action == "failed":
                        logger.error(f"{worker_display_name} : [BASELINE] {details}")
                        failed += 1
                    # 'skipped' counts are ignored 
                except Exception as e:
                    logger.error(f"Worker exception: {e}")
                    failed += 1

        logger.info(
            f"[BASELINE] Done | created={created} updated={updated} failed={failed}"
        )
        
        return {
            "created": created,
            "updated": updated,
            "failed": failed,
        }
