# crawler/baseline_worker.py

from crawler.fetcher import fetch
from crawler.storage.crawl_reader import iter_crawl_urls
from crawler.storage.baseline_store import save_baseline
from crawler.normalizer import normalize_url


class BaselineWorker:
    def __init__(self, *, custid, siteid, seed_url):
        self.custid = custid
        self.siteid = siteid
        self.seed_url = seed_url

    def run(self):
        print("[BASELINE] Controlled baseline refetch started")

        urls = iter_crawl_urls(siteid=self.siteid)

        # ✅ NEW LOG
        if not urls:
            print(
                f"[BASELINE][WARN] No crawl_pages data found for site_id={self.siteid}. "
                "Run CRAWL mode first."
            )
            return

        created = 0
        updated = 0
        failed = 0

        for url in urls:
            try:
                fetch_url = normalize_url(
                    url,
                    preference_url=self.seed_url
                )

                result = fetch(fetch_url)
                if not result["success"]:
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
                    created += 1
                else:
                    updated += 1


            except Exception as e:
                failed += 1

        if updated == 0:
            print(
                f"[BASELINE] Done | created={created} failed={failed}"
            )
        else:
            print(
                f"[BASELINE] Done | updated={updated} failed={failed}"
            )

