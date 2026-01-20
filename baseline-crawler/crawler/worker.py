# crawler/worker.py
import threading
import time
import re
from collections import defaultdict
from urllib.parse import urlparse

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
from crawler.js_renderer import render_js_sync
from crawler.render_cache import get_cached_render, set_cached_render


# ==================================================
# BLOCK RULES
# ==================================================

PATH_BLOCK_RULES = {
    "TAG_PAGE": r"^/tag/",
    "AUTHOR_PAGE": r"^/author/",
    "PAGINATION": r"/page/\d*/?$",
}

STATIC_EXTENSIONS = (
    ".css", ".js", ".png", ".jpg", ".jpeg",
    ".gif", ".svg", ".ico", ".pdf", ".zip"
)

BLOCK_REPORT = defaultdict(list)
BLOCK_LOCK = threading.Lock()


def classify_block(url: str):
    parsed = urlparse(url)
    if parsed.path.endswith(STATIC_EXTENSIONS):
        return "STATIC"
    for k, r in PATH_BLOCK_RULES.items():
        if re.search(r, parsed.path.lower()):
            return k
    return None


# ==================================================
# STRICT DOMAIN FILTER
# ==================================================

def _allowed_domain(seed_url: str, candidate_url: str) -> bool:
    """Check if candidate URL is from the same domain as seed."""
    from urllib.parse import urlparse
    seed_domain = urlparse(seed_url).netloc.lower()
    cand_domain = urlparse(candidate_url).netloc.lower()
    return seed_domain == cand_domain


def _is_home_page(seed_url: str, url: str) -> bool:
    """
    Check if url is the home page (same as seed_url).
    This skips storing duplicate home page entries.
    """
    seed_parsed = urlparse(seed_url.rstrip('/'))
    url_parsed = urlparse(url.rstrip('/'))
    
    # Same domain and same path (both are home pages)
    return (seed_parsed.netloc == url_parsed.netloc and 
            seed_parsed.path.rstrip('/') == url_parsed.path.rstrip('/'))


def _extract_relative_url(full_url: str) -> str:
    """
    Extract relative URL from full URL, remove trailing slash.
    Pure char only - no trailing slashes in stored URLs.
    Examples:
    - https://hocco.in/ → / (home page, will be skipped)
    - https://hocco.in/about-us/ → /about-us
    - https://hocco.in/about-us → /about-us
    - https://hocco.in/products/item-1/ → /products/item-1
    """
    parsed = urlparse(full_url)
    # Strip trailing slashes from path
    path = parsed.path.rstrip('/') or '/'
    
    # Add query string if present
    if parsed.query:
        path += f"?{parsed.query}"
    
    return path

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
    ):
        super().__init__(name=name)
        self.frontier = frontier
        self.running = True
        self.custid = custid
        self.siteid = next(iter(siteid_map.values()))
        self.job_id = job_id
        self.crawl_mode = crawl_mode
        self.seed_url = seed_url

        self.compare_engine = (
            CompareEngine(custid=self.custid)
            if crawl_mode == "COMPARE"
            else None
        )

    def run(self):
        print(f"[{self.name}] 🚀 STARTED in {self.crawl_mode} mode")
        print(f"[{self.name}] 📋 Seed URL: {self.seed_url}")
        print(f"[{self.name}] 🏢 Customer: {self.custid}, Site: {self.siteid}")

        while self.running:
            (item, got_task) = self.frontier.dequeue()

            if not got_task:
                time.sleep(0.1)
                continue

            url, parent = item
            start = time.time()

            try:
                print(f"\n[{self.name}] 🔍 Crawling: {url}")

                result = fetch(url, parent)
                print(f"[{self.name}] ✅ Fetch completed for {url}")

                if not result["success"]:
                    print(f"[{self.name}] ❌ Fetch failed for {url}")
                    continue

                resp = result["response"]
                ct = resp.headers.get("Content-Type", "")
                print(f"[{self.name}] 📄 Content-Type: {ct}, Status: {resp.status_code}")

                # Skip inserting home page entry (already in sites table)
                if not _is_home_page(self.seed_url, url):
                    # Extract relative URL and store without trailing slash
                    relative_url = _extract_relative_url(url)
                    print(f"[{self.name}] 💾 Storing page: {relative_url}")
                    
                    insert_crawl_page({
                        "job_id": self.job_id,
                        "custid": self.custid,
                        "siteid": self.siteid,
                        "url": relative_url,
                        "status_code": resp.status_code,
                        "content_type": ct,
                    })
                    print(f"[{self.name}] ✅ Page stored in DB")
                else:
                    print(f"[{self.name}] ⏭️  Skipping home page (already in sites table)")

                if "text/html" not in ct.lower():
                    print(f"[{self.name}] ⏭️  Skipping non-HTML content: {ct}")
                    continue

                html = resp.text
                print(f"[{self.name}] 📝 HTML size: {len(html)} bytes")

                # Skip JS rendering for now due to greenlet threading issues
                # if needs_js_rendering(html):
                #     print(f"[{self.name}] 🎭 JS rendering required for {url}")
                #     cached = get_cached_render(url)
                #     if cached:
                #         print(f"[{self.name}] ♻️  Using cached render")
                #         html = cached
                #     else:
                #         print(f"[{self.name}] 🔄 Rendering JS with Playwright...")
                #         html = normalize_rendered_html(render_js_sync(url))
                #         set_cached_render(url, html)
                #         print(f"[{self.name}] ✅ JS render complete, cached")

                urls, _ = extract_urls(html, url)
                print(f"[{self.name}] 🔗 Extracted {len(urls)} URLs from page")

                if self.crawl_mode == "BASELINE":
                    print(f"[{self.name}] 📸 BASELINE MODE: Storing snapshot for {url}")
                    baseline_id, _, path = store_snapshot_file(
                        custid=self.custid,
                        siteid=self.siteid,
                        url=url,
                        html=html,
                        crawl_mode="BASELINE",
                    )
                    print(f"[{self.name}] ✅ Baseline snapshot stored: {path}")

                    store_baseline_hash(
                        site_id=self.siteid,
                        normalized_url=normalize_url(url),
                        raw_html=html,
                        baseline_path=path,
                    )
                    print(f"[{self.name}] ✅ Baseline hash stored in DB")

                elif self.crawl_mode == "COMPARE":
                    print(f"[{self.name}] 🔍 COMPARE MODE: Checking for defacement on {url}")
                    self.compare_engine.handle_page(
                        siteid=self.siteid,
                        url=url,
                        html=html,
                    )
                    print(f"[{self.name}] ✅ Defacement check complete")
                
                elif self.crawl_mode == "CRAWL":
                    print(f"[{self.name}] 🏃 CRAWL MODE: No baseline/comparison needed")

                print(f"[{self.name}] 🔄 Processing {len(urls)} extracted URLs...")
                enqueued_count = 0
                blocked_count = 0
                domain_filtered = 0
                
                for u in urls:
                    if classify_block(u):
                        blocked_count += 1
                        with BLOCK_LOCK:
                            BLOCK_REPORT["BLOCK_RULE"].append(u)
                        continue

                    if not _allowed_domain(self.seed_url, u):
                        domain_filtered += 1
                        with BLOCK_LOCK:
                            BLOCK_REPORT["DOMAIN_FILTER"].append(u)
                        continue

                    self.frontier.enqueue(u, url)
                    enqueued_count += 1
                
                print(f"[{self.name}] 📊 URLs - Enqueued: {enqueued_count}, Blocked: {blocked_count}, Domain filtered: {domain_filtered}")

            except Exception as e:
                print(f"[{self.name}] ❌ ERROR {url}: {e}")
                import traceback
                traceback.print_exc()

            finally:
                self.frontier.mark_visited(url, got_task=got_task)
                print(f"[{self.name}] ✔️  Marked {url} as visited")

    def stop(self):
        print(f"[{self.name}] 🛑 Stopping worker")
        self.running = False
