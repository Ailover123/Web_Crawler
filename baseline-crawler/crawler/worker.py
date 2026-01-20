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

    # Block static file extensions first
    if parsed.path.endswith(STATIC_EXTENSIONS):
        return "STATIC"

    # Block blog pages with query parameters like "?e-page-765f5351=12"
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
    """
    Strict domain check. Since normalize_url now handles branding preferences,
    we just need to check if the netlocs match exactly after preference-aware normalization.
    """
    from crawler.normalizer import normalize_url
    
    # Normalize BOTH with the same seed_url as preference
    s_norm = normalize_url(seed_url, preference_url=seed_url)
    c_norm = normalize_url(candidate_url, preference_url=seed_url)
    
    s_netloc = urlparse(s_norm).netloc.lower().split(":")[0]
    c_netloc = urlparse(c_norm).netloc.lower().split(":")[0]

    return s_netloc == c_netloc


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
                    # Suppress noisy messages for ignored content types (e.g., images)
                    if isinstance(err, str) and "ignored content type" in err:
                        with BLOCK_LOCK:
                            BLOCK_REPORT["FETCH_IGNORED_CONTENT_TYPE"]["count"] += 1
                            BLOCK_REPORT["FETCH_IGNORED_CONTENT_TYPE"]["urls"].append(url)
                        continue
                    print(f"[{self.name}] Fetch failed for {url}: {err}")
                    continue

                resp = result["response"]
                ct = resp.headers.get("Content-Type", "")

                insert_crawl_page({
                    "job_id": self.job_id,
                    "custid": self.custid,
                    "siteid": self.siteid,
                    "url": url,
                    "parent_url": parent,
                    "depth": depth,
                    "status_code": resp.status_code,
                    "content_type": ct,
                    "content_length": len(resp.content),
                    "response_time_ms": int((time.time() - start) * 1000),
                    "fetched_at": fetched_at,
                    "base_url": self.seed_url, # Pass preference
                })

                if "text/html" not in ct.lower():
                    continue

                # ---------------- HTML HANDLING ----------------
                html = resp.text

                # 🔒 ALWAYS ensure final HTML before extracting URLs
                urls, _ = extract_urls(html, url)

                if not urls and needs_js_rendering(html):

                    cached = get_cached_render(url)
                    if cached:
                        html = cached
                    else:
                        print(f"[{self.name}] JS rendering {url}")
                        html = JS_RENDERER.render(url)
                        set_cached_render(url, html)


                # 🔒 Extract URLs ONLY after JS handling
                urls, _ = extract_urls(html, url)

                if not urls:
                    print(f"[{self.name}] ⚠️  No URLs extracted from {url}")
                    print(f"[{self.name}]    HTML size: {len(html)} bytes")
                    print(f"[{self.name}]    Possible cause: JS-rendered content or minimal links")
                else:
                    print(f"[{self.name}] Extracted {len(urls)} URLs from {url}")

                # ---------------- MODE LOGIC ----------------
                if self.crawl_mode == "BASELINE":
                    baseline_id, _, path = store_snapshot_file(
                        custid=self.custid,
                        siteid=self.siteid,
                        url=url,
                        html=html,
                        crawl_mode="BASELINE",
                        base_url=self.seed_url,
                    )

                    store_baseline_hash(
                        site_id=self.siteid,
                        normalized_url=normalize_url(url, preference_url=self.seed_url),
                        raw_html=html,
                        baseline_path=path,
                        base_url=self.seed_url,
                    )

                elif self.crawl_mode == "COMPARE":
                    self.compare_engine.handle_page(
                        siteid=self.siteid,
                        url=url,
                        html=html,
                        base_url=self.seed_url,
                    )

                # ---------------- ENQUEUE ----------------
                enqueued_count = 0
                blocked_rule_count = 0
                blocked_domain_count = 0
                for u in urls:
                    block_type = classify_block(u)
                    if block_type:
                        with BLOCK_LOCK:
                            BLOCK_REPORT[block_type]["count"] += 1
                            BLOCK_REPORT[block_type]["urls"].append(u)
                        blocked_rule_count += 1
                        continue

                    if not _allowed_domain(self.seed_url, u):
                        with BLOCK_LOCK:
                            BLOCK_REPORT["DOMAIN_FILTER"]["count"] += 1
                            BLOCK_REPORT["DOMAIN_FILTER"]["urls"].append(u)
                        blocked_domain_count += 1
                        continue

                    self.frontier.enqueue(u, url, depth + 1, preference_url=self.seed_url)
                    enqueued_count += 1

                if enqueued_count > 0:
                    print(f"[{self.name}] Enqueued {enqueued_count} URLs")

                # Print a concise summary of blocked URLs instead of every single one
                if blocked_rule_count or blocked_domain_count:
                    parts = []
                    if blocked_rule_count:
                        parts.append(f"{blocked_rule_count} blocked by rule")
                    if blocked_domain_count:
                        parts.append(f"{blocked_domain_count} blocked by domain")
                    print(f"[{self.name}] Blocked: {'; '.join(parts)}")

            except Exception as e:
                import traceback
                print(f"[{self.name}] ERROR {url}: {e}")
                print(f"[{self.name}] Traceback: {traceback.format_exc()}")

            finally:
                self.frontier.mark_visited(url, got_task=got_task, preference_url=self.seed_url)

    def stop(self):
        self.running = False
