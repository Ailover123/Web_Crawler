# crawler/baseline_worker.py

from crawler.fetcher import fetch
from crawler.storage.crawl_reader import iter_crawl_urls
from crawler.storage.baseline_store import save_baseline_if_unique
from crawler.normalizer import normalize_url
from crawler.logger import logger


class BaselineWorker:
    def __init__(self, *, custid, siteid, seed_url):
        self.custid = custid
        self.siteid = siteid
        self.seed_url = seed_url

    def run(self):
        logger.info(f"[BASELINE] Controlled baseline refetch started for site_id={self.siteid}")

        urls = iter_crawl_urls(siteid=self.siteid)

        if not urls:
            logger.warning(
                f"[BASELINE] No crawl_pages data found for site_id={self.siteid}. "
                "Run CRAWL mode first."
            )
            return

        created = 0
        skipped = 0
        failed = 0

        for url in urls:
            try:
                fetch_url = normalize_url(
                    url,
                    preference_url=self.seed_url
                )

                result = fetch(fetch_url)
                if not result["success"]:
                    logger.error(f"[BASELINE] Fetch failed for {fetch_url}: {result.get('error')}")
                    failed += 1
                    continue

                resp = result["response"]
                ct = resp.headers.get("Content-Type", "").lower()
                if "text/html" not in ct:
                    continue

                baseline_id, path = save_baseline_if_unique(
                    custid=self.custid,
                    siteid=self.siteid,
                    url=url,
                    html=resp.text,
                    base_url=self.seed_url,
                )

                if baseline_id:
                    logger.info(f"[BASELINE] Saved baseline {baseline_id} for {url}")
                    created += 1
                else:
                    skipped += 1

            except Exception as e:
                logger.error(f"[BASELINE] Error processing {url}: {e}")
                failed += 1

        logger.info(
            f"[BASELINE] Done | created={created} skipped={skipped} failed={failed}"
        )
