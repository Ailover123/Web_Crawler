# crawler/worker.py
import threading
import time
import re
from collections import defaultdict
from urllib.parse import urlparse
from datetime import datetime, timezone

from crawler.fetcher import fetch
from crawler.parser import extract_urls
from crawler.normalizer import (
    normalize_rendered_html,
    normalize_url,
)
from crawler.storage.db import insert_crawl_page
from crawler.storage.baseline_store import (
    store_snapshot_file,
    store_baseline_hash,
)
from crawler.compare_engine import CompareEngine

from crawler.js_detect import needs_js_rendering
from crawler.render_cache import get_cached_render, set_cached_render
from crawler.js_render_worker import JSRenderWorker

JS_RENDERER = JSRenderWorker()


# ==================================================
# BLOCK RULES
# ==================================================

PATH_BLOCK_RULES = {
    "TAG_PAGE": r"^/tag/",
    "AUTHOR_PAGE": r"^/author/",
    "PAGINATION": r"/page/\d*/?$",
    "ASSET_DIRECTORY": r"^/(assets|static|media|uploads|images|img|css|js)/",
}

QUERY_BLOCK_RULES = {
    "ELEMENTOR_PAGINATION": r"^e-page-",
    "GENERIC_PAGINATION": r"^(page|paged|p)$",
}

STATIC_EXTENSIONS = (
    ".css", ".js", ".png", ".jpg", ".jpeg", ".webp",
    ".gif", ".svg", ".ico", ".woff", ".woff2",
    ".ttf", ".eot", ".pdf", ".zip"
)

BLOCK_REPORT = defaultdict(lambda: {"count": 0, "urls": []})
BLOCK_LOCK = threading.Lock()


def classify_block(url: str):
    parsed = urlparse(url)

    if parsed.path.endswith(STATIC_EXTENSIONS):
        return "STATIC"

    if parsed.query:
        if re.search(r'(^|&)(e-page-[0-9a-fA-F]+)=', parsed.query):
            return "BLOG_EPAGE"

    for k, r in PATH_BLOCK_RULES.items():
        if re.search(r, parsed.path.lower()):
            return k

    return None


# ==================================================
# STRICT DOMAIN FILTER
# ==================================================

def _allowed_domain(seed_url: str, candidate_url: str) -> bool:
    seed_netloc = urlparse(seed_url).netloc.lower().split(":")[0]
    cand_netloc = urlparse(candidate_url).netloc.lower().split(":")[0]

    base = seed_netloc[4:] if seed_netloc.startswith("www.") else seed_netloc
    return cand_netloc == base or cand_netloc == f"www.{base}"


# ==================================================
# WORKER
# ==================================================

class Worker(threading.Thread):
    def __init__(
        self,
        frontier,
        name,
        custid,
        siteid_map,
        job_id,
        crawl_mode,
        seed_url,
        original_site_url=None,   # ✅ DB identity
    ):
        super().__init__(name=name)
        self.frontier = frontier
        self.running = True
        self.custid = custid
        self.siteid = next(iter(siteid_map.values()))
        self.job_id = job_id
        self.crawl_mode = crawl_mode
        self.seed_url = seed_url
        self.original_site_url = original_site_url

        self.compare_engine = (
            CompareEngine(custid=self.custid)
            if crawl_mode == "COMPARE"
            else None
        )

    # --------------------------------------------------
    # DB URL IDENTITY FIX
    # --------------------------------------------------
    def _db_url(self, fetched_url: str) -> str:
        """
        Store URLs WITHOUT scheme.
        Preserve www only if original site URL had it.
        """
        try:
            parsed = urlparse(fetched_url)

            host = parsed.netloc.lower()

            # Decide whether www should be kept
            keep_www = False
            if self.original_site_url:
                keep_www = urlparse(
                    normalize_url(self.original_site_url)
                ).netloc.lower().startswith("www.")

            # Remove www if original site didn't have it
            if host.startswith("www.") and not keep_www:
                host = host[4:]

            # Rebuild URL WITHOUT scheme
            path = parsed.path or ""
            query = f"?{parsed.query}" if parsed.query else ""

            return f"{host}{path}{query}"

        except Exception:
            # absolute fallback
            return fetched_url


    def run(self):
        print(f"[{self.name}] started ({self.crawl_mode})")

        while self.running:
            (item, got_task) = self.frontier.dequeue()

            if not got_task:
                time.sleep(0.1)
                continue

            url, parent, depth = item
            start = time.time()

            try:
                print(f"[{self.name}] Crawling {url}")

                result = fetch(url, parent, depth)
                fetched_at = datetime.now(timezone.utc)

                if not result["success"]:
                    err = result.get("error", "unknown")
                    if isinstance(err, str) and "ignored content type" in err:
                        with BLOCK_LOCK:
                            BLOCK_REPORT["FETCH_IGNORED_CONTENT_TYPE"]["count"] += 1
                            BLOCK_REPORT["FETCH_IGNORED_CONTENT_TYPE"]["urls"].append(url)
                        continue
                    print(f"[{self.name}] Fetch failed for {url}: {err}")
                    continue

                resp = result["response"]
                ct = resp.headers.get("Content-Type", "")

                # ✅ STORE DB URL CORRECTLY
                insert_crawl_page({
                    "job_id": self.job_id,
                    "custid": self.custid,
                    "siteid": self.siteid,
                    "url": self._db_url(url),
                    "parent_url": self._db_url(parent) if parent else None,
                    "depth": depth,
                    "status_code": resp.status_code,
                    "content_type": ct,
                    "content_length": len(resp.content),
                    "response_time_ms": int((time.time() - start) * 1000),
                    "fetched_at": fetched_at,
                })

                if "text/html" not in ct.lower():
                    continue

                html = resp.text

                urls, _ = extract_urls(html, url)

                if not urls and needs_js_rendering(html):
                    cached = get_cached_render(url)
                    if cached:
                        html = cached
                    else:
                        print(f"[{self.name}] JS rendering {url}")
                        html = JS_RENDERER.render(url)
                        set_cached_render(url, html)

                urls, _ = extract_urls(html, url)

                if self.crawl_mode == "BASELINE":
                    baseline_id, _, path = store_snapshot_file(
                        custid=self.custid,
                        siteid=self.siteid,
                        url=self._db_url(url),
                        html=html,
                        crawl_mode="BASELINE",
                    )

                    store_baseline_hash(
                        site_id=self.siteid,
                        normalized_url=normalize_url(url),
                        raw_html=html,
                        baseline_path=path,
                    )

                elif self.crawl_mode == "COMPARE":
                    self.compare_engine.handle_page(
                        siteid=self.siteid,
                        url=url,
                        html=html,
                    )

                enqueued = 0
                for u in urls:
                    block_type = classify_block(u)
                    if block_type:
                        with BLOCK_LOCK:
                            BLOCK_REPORT[block_type]["count"] += 1
                            BLOCK_REPORT[block_type]["urls"].append(u)
                        continue

                    if not _allowed_domain(self.seed_url, u):
                        with BLOCK_LOCK:
                            BLOCK_REPORT["DOMAIN_FILTER"]["count"] += 1
                            BLOCK_REPORT["DOMAIN_FILTER"]["urls"].append(u)
                        continue

                    self.frontier.enqueue(u, url, depth + 1)
                    enqueued += 1

                if enqueued:
                    print(f"[{self.name}] Enqueued {enqueued} URLs")

            except Exception as e:
                import traceback
                print(f"[{self.name}] ERROR {url}: {e}")
                print(traceback.format_exc())

            finally:
                self.frontier.mark_visited(url, got_task=got_task)

    def stop(self):
        self.running = False
