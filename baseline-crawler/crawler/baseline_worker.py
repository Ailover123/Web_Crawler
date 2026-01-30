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

        for url in urls:
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
                    failed += 1
                    continue

                resp = result["response"]
                ct = resp.headers.get("Content-Type", "").lower()
                if "text/html" not in ct:
                    continue

                baseline_id, path, action = save_baseline(
                    custid=self.custid,
                    siteid=self.siteid,
                    url=url,
                    html=resp.text,
                    base_url=self.seed_url,
                )

                if action == "created":
                    logger.info(f"[BASELINE] Created baseline {baseline_id} for {url}")
                    created += 1
                elif action == "updated":
                    logger.info(f"[BASELINE] Updated baseline {baseline_id} for {url}")
                    updated += 1
                else:
                    logger.info(f"[BASELINE] No change for {url}")

            except Exception as e:
                logger.error(f"[BASELINE] Error processing {url}: {e}")
                failed += 1

        logger.info(
            f"[BASELINE] Done | created={created} updated={updated} failed={failed}"
        )
        
        return {
            "created": created,
            "updated": updated,
            "failed": failed,
        }
