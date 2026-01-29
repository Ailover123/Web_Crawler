import logging
import threading
import time
import re
from collections import defaultdict
from urllib.parse import urlparse
from datetime import datetime, timezone
from crawler.config import CRAWL_DELAY
from crawler.fetcher import fetch
from crawler.parser import extract_urls
from crawler.normalizer import (
    normalize_rendered_html,
    normalize_url,
)
from crawler.url_utils import force_www_url
from crawler.storage.db import insert_crawl_page
from crawler.storage.baseline_store import (
    save_baseline,
)
from crawler.compare_engine import CompareEngine

from crawler.logger import logger

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
    "ELEMENTOR_PAGINATION": r"e-page-",
    "GENERIC_PAGINATION": r"(page|paged|p)",
}

STATIC_EXTENSIONS = (
    ".css", ".js", ".png", ".jpg", ".jpeg", ".webp",
    ".gif", ".svg", ".ico", ".woff", ".woff2",
    ".ttf", ".eot", ".pdf", ".zip", ".xlsx",
    ".xls", ".docx", ".doc", ".gz", ".tar",
    ".ppt", ".pptx", ".mp3"
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
        # Check hardcoded e-page rule
        if re.search(r'(^|&)(e-page-[0-9a-fA-F]+)=', parsed.query):
            return "BLOG_EPAGE"
            
        # Check defined QUERY_BLOCK_RULES
        for k, r in QUERY_BLOCK_RULES.items():
            # Create a regex to match the parameter name followed by '='
            # The rule 'r' matches the key name, e.g. "^(page|paged|p)$"
            # We look for: (Start or &) + (Rule) + (=)
            pattern = fr'(^|&){r}='
            if re.search(pattern, parsed.query.lower()):
                return k

    for k, r in PATH_BLOCK_RULES.items():
        if re.search(r, parsed.path.lower()):
            return k

    return None


# ==================================================
# STRICT DOMAIN FILTER
# ==================================================

def _allowed_domain(seed_url: str, candidate_url: str, current_url: str = None) -> bool:
    """
    Strict domain check. Only allows URLs that exactly match the netloc of the seed
    or the current page (if different due to redirect).
    """
    s_netloc = urlparse(seed_url).netloc.lower().split(":")[0]
    c_netloc = urlparse(candidate_url).netloc.lower().split(":")[0]

    if s_netloc == c_netloc:
        return True

    if current_url:
        curr_netloc = urlparse(current_url).netloc.lower().split(":")[0]
        if curr_netloc == c_netloc:
            return True

    return False


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
        
        # Stats
        self.saved_count = 0
        self.duplicate_count = 0
        self.failed_count = 0
        self.policy_skipped_count = 0
        self.frontier_duplicate_count = 0

    def _log(self, level, msg, *args, **kwargs):
        """Helper to log with worker names as context."""
        kwargs.setdefault('extra', {})
        kwargs['extra']['context'] = self.name
        logger.log(level, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self._log(logging.INFO, msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._log(logging.ERROR, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._log(logging.WARNING, msg, *args, **kwargs)

    def is_soft_redirect(self, html: str) -> bool:
        """
        Heuristic to detect soft redirects (Meta-refresh or JS window.location).
        """
        if not html:
            return False
        h = html.lower()
        return 'http-equiv="refresh"' in h or 'window.location' in h

    def run(self):
        self.info(f"started ({self.crawl_mode})")

        while self.running:
            (item, got_task) = self.frontier.dequeue()

            if not got_task:
                time.sleep(0.1)
                continue

            url, parent, depth = item
            
            # --- CRAWL DELAY ---
            if CRAWL_DELAY > 0:
                time.sleep(CRAWL_DELAY)
                
            start = time.time()

            try:
                self.info(f"Crawling {url}")

                # 🔒 Force www prefix for the network fetch
                fetch_url = force_www_url(url)
                
                result = fetch(fetch_url, parent, depth)
                fetched_at = datetime.now()

                if not result["success"]:
                    err = result.get("error", "unknown")
                    # Suppress noisy messages for ignored content types (e.g., images)
                    if isinstance(err, str) and "ignored content type" in err:
                        with BLOCK_LOCK:
                            BLOCK_REPORT["FETCH_IGNORED_CONTENT_TYPE"]["count"] += 1
                            BLOCK_REPORT["FETCH_IGNORED_CONTENT_TYPE"]["urls"].append(url)
                        continue

                    # SPECIAL CASE: 404 with Meta-refresh or JS Redirect (Soft Redirect)
                    if "http error: 404" in str(err) and "html" in result:
                        if self.is_soft_redirect(result["html"]):
                            final_html, final_url = JS_RENDERER.render(url)
                            
                            if final_url and final_url != url:
                                self.info(f"Soft Redirect detected on 404 for {url} -> Redirected to {final_url} (Recovered)")
                                result["success"] = True
                                result["response"] = type('obj', (object,), {'status_code': 200, 'headers': {'Content-Type': 'text/html'}, 'text': final_html, 'content': final_html.encode()})
                                result["final_url"] = final_url
                                ct = "text/html"
                            else:
                                self.info(f"Soft Redirect failed for {url} (Still 404 / No redirect after waiting)")
                                if final_html:
                                    result["html"] = final_html

                    if not result["success"]:
                        self.error(f"Fetch failed for {url}: {err}")
                        self.failed_count += 1
                        continue

                resp = result["response"]
                ct = resp.headers.get("Content-Type", "")
                final_url = result.get("final_url", url)

                # Log redirect if it happened
                if final_url != url:
                    self.info(f"[REDIRECT] Original URL was {url} but it redirected to {final_url}")

                db_result = insert_crawl_page({
                    "job_id": self.job_id,
                    "custid": self.custid,
                    "siteid": self.siteid,
                    "url": final_url, # 🔒 Use final destination URL
                    "parent_url": parent,
                    "depth": depth,
                    "status_code": resp.status_code,
                    "content_type": ct,
                    "content_length": len(resp.content),
                    "response_time_ms": int((time.time() - start) * 1000),
                    "fetched_at": fetched_at,
                    "base_url": self.seed_url, # Pass preference
                })

                if not db_result:
                    # Root URL skipped by policy in insert_crawl_page
                    self.policy_skipped_count += 1
                    self.info(f"DB: Skipped crawl_pages for {url} (policy)")
                    continue

                db_action = db_result["action"]
                affected_id = db_result["id"]

                if db_action == "Inserted":
                    # Only increment saved_count if it's a genuine NEW insertion
                    if self.crawl_mode in ("CRAWL", "BASELINE"):
                        self.saved_count += 1
                    self.info(f"DB: Inserted crawl_pages for {final_url} (ID: {affected_id})")
                elif db_action == "Updated":
                    # Canonical duplicate (e.g. http vs https pair)
                    self.duplicate_count += 1
                    self.info(f"DB: Updated crawl_pages for {final_url} (ID: {affected_id})")
                else:
                    self.info(f"DB: Action {db_action} for {url}")

                if "text/html" not in ct.lower():
                    continue

                # ---------------- HTML HANDLING ----------------
                html = resp.text

                # 🔒 Extract URLs using final destination as base
                # This ensures relative links are joined correctly if a redirect happened.
                urls, _ = extract_urls(html, final_url)

                if not urls and needs_js_rendering(html):

                    cached = get_cached_render(final_url) # Use final_url for cache
                    if cached:
                        html = cached
                    else:
                        self.info(f"JS rendering {final_url}")
                        # 🔒 FIX TypeError: Unpack tuple (html, rendered_url)
                        html, rendered_url = JS_RENDERER.render(final_url)
                        if rendered_url:
                            final_url = rendered_url
                        set_cached_render(final_url, html)


                # 🔒 Extract URLs ONLY after JS handling, using the latest final_url
                urls, _ = extract_urls(html, final_url)

                if not urls:
                    self.warning(f"[WARN] No URLs extracted from {final_url}")
                    self.warning(f"   HTML size: {len(html)} bytes")
                    self.warning(f"   Possible cause: JS-rendered content or minimal links")
                else:
                    self.info(f"Extracted {len(urls)} URLs from {final_url}")

                # ---------------- MODE LOGIC ----------------
                if self.crawl_mode == "BASELINE":
                    baseline_id, path, action = save_baseline(
                        custid=self.custid,
                        siteid=self.siteid,
                        url=final_url, # Use final_url for baseline
                        html=html,
                        base_url=self.seed_url,
                    )

                    if baseline_id:
                        self.info(f"DB: {action.upper()} baseline for {final_url} with ID {baseline_id}")


                elif self.crawl_mode == "COMPARE":
                    self.compare_engine.handle_page(
                        siteid=self.siteid,
                        url=final_url,
                        html=html,
                        base_url=self.seed_url,
                    )
                
                else: # CRAWL mode
                    pass # Handled by insert_crawl_page logic above

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

                    # Allow domain change if it's the destination of the seed OR matches seed domain
                    if not _allowed_domain(self.seed_url, u, current_url=final_url):
                        with BLOCK_LOCK:
                            BLOCK_REPORT["DOMAIN_FILTER"]["count"] += 1
                            BLOCK_REPORT["DOMAIN_FILTER"]["urls"].append(u)
                        blocked_domain_count += 1
                        continue

                    if self.frontier.enqueue(u, final_url, depth + 1, preference_url=self.seed_url):
                        enqueued_count += 1
                    else:
                        self.frontier_duplicate_count += 1

                if enqueued_count > 0:
                    self.info(f"Enqueued {enqueued_count} URLs")

                # Print a concise summary of blocked URLs instead of every single one
                if blocked_rule_count or blocked_domain_count:
                    parts = []
                    if blocked_rule_count:
                        parts.append(f"{blocked_rule_count} blocked by rule")
                    if blocked_domain_count:
                        parts.append(f"{blocked_domain_count} blocked by domain")
                    self.info(f"Blocked: {'; '.join(parts)}")

            except Exception as e:
                import traceback
                self.error(f"ERROR {url}: {e}")
                self.error(f"Traceback: {traceback.format_exc()}")

            finally:
                self.frontier.mark_visited(url, got_task=got_task, preference_url=self.seed_url)

    def stop(self):
        self.running = False
