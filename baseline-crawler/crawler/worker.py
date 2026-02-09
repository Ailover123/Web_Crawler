import logging
import threading
import time
import re
from collections import defaultdict
from urllib.parse import urlparse
from datetime import datetime
from crawler.config import CRAWL_DELAY
from crawler.fetcher import fetch
from crawler.parser import extract_urls
from crawler.normalizer import normalize_url
from crawler.url_utils import force_www_url
from crawler.storage.db import insert_crawl_page
from crawler.storage.baseline_store import save_baseline
from crawler.compare_engine import CompareEngine
from crawler.logger import logger
from crawler.js_detect import needs_js_rendering
from crawler.render_cache import get_cached_render, set_cached_render
from crawler.js_render_worker import JSRenderWorker
from crawler.throttle import get_remaining_pause, set_pause

JS_RENDERER = JSRenderWorker()

# Thread-safe global for crawler-level reporting
SKIP_REPORT = defaultdict(lambda: {"count": 0, "urls": []})
SKIP_LOCK = threading.Lock()


# ==================================================
# SKIP RULES
# ==================================================

PATH_SKIP_RULES = {
    "TAG_PAGE": r"^/(product-)?tag/",
    "AUTHOR_PAGE": r"^/author/",
    "PAGINATION": r"/page/\d*/?$",
    "ASSET_DIRECTORY": r"^/(assets|static|media|uploads|images|img|css|js)/",
}

QUERY_SKIP_RULES = {
    "PAGINATION": r"(^|&)(page|paged|p)=",
    "SORTING": r"(orderby|sort|order|filter_|display)=",
    "ACTIONS": r"(add-to-cart|add_to_wishlist|action=yith-woocompare|remove_item)",
    "SITE_QUERY": r"(^|&)site=",
    "GL_TRACKING": r"(^|&)_gl=",
    "UTM_MARKETING": r"(^|&)utm_",
}

STATIC_EXTENSIONS = (
    ".css", ".js", ".png", ".jpg", ".jpeg", ".webp",
    ".gif", ".svg", ".ico", ".woff", ".woff2",
    ".ttf", ".eot", ".pdf", ".zip", ".xlsx",
    ".xls", ".docx", ".doc", ".gz", ".tar",
    ".ppt", ".pptx", ".mp3"
)


def classify_skip(url: str):
    parsed = urlparse(url)
    
    path = parsed.path if parsed.path else ""

    if path.endswith(STATIC_EXTENSIONS):
        return "STATIC"

    if parsed.query:
        if re.search(r'(^|&)(e-page-[0-9a-f_A-F]+)=', parsed.query):
            return "BLOG_EPAGE"
        for k, r in QUERY_SKIP_RULES.items():
            if re.search(r, parsed.query.lower()):
                return k

    for k, r in PATH_SKIP_RULES.items():
        if re.search(r, path.lower()):
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

    # Also allow standard www/non-www transition if they match the base
    s_base = s_netloc[4:] if s_netloc.startswith("www.") else s_netloc
    c_base = c_netloc[4:] if c_netloc.startswith("www.") else c_netloc
    
    return s_base == c_base


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
        skip_report=None,
        skip_lock=None,
        target_urls=None,
        compare_results=None,  # New param for collecting results
        compare_lock=None,
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
        self.skip_report = skip_report if skip_report is not None else SKIP_REPORT
        self.skip_lock = skip_lock if skip_lock is not None else SKIP_LOCK
        
        self.compare_results = compare_results
        self.compare_lock = compare_lock

        self.saved_count = 0
        self.failed_count = 0
        self.failed_429_count = 0
        self.existed_count = 0
        self.policy_skipped_count = 0
        self.frontier_duplicate_count = 0

        self.target_urls = target_urls # If present, disables recursive crawling

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
                nl = urlparse(self.original_site_url).netloc.lower()
                if not nl.startswith("http"): # handle potential missing scheme in original_site_url
                     nl = urlparse("https://" + self.original_site_url).netloc.lower()
                keep_www = nl.startswith("www.")

            # Remove www if original site didn't have it
            if host.startswith("www.") and not keep_www:
                host = host[4:]

            # Rebuild URL WITHOUT scheme
            path = parsed.path or ""
            query = f"?{parsed.query}" if parsed.query else ""

            return f"{host}{path}{query}"

        except Exception:
            return fetched_url

    def info(self, msg, *args, **kwargs):
        kwargs.setdefault('extra', {})
        kwargs['extra']['context'] = self.name
        logger.info(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        kwargs.setdefault('extra', {})
        kwargs['extra']['context'] = self.name
        logger.error(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        kwargs.setdefault('extra', {})
        kwargs['extra']['context'] = self.name
        logger.warning(msg, *args, **kwargs)

    def is_soft_redirect(self, html: str) -> bool:
        if not html: return False
        h = html.lower()
        return 'http-equiv="refresh"' in h or 'window.location' in h

    def run(self):
        self.info(f"started ({self.crawl_mode})")

        while self.running:
            # --- GLOBAL DOMAIN PAUSE CHECK ---
            remaining = get_remaining_pause(self.siteid)
            if remaining > 0:
                self.info(f"Global Pause active for site {self.siteid}. Waiting {remaining:.1f}s more...")
                time.sleep(1.0) # Check every 1s to be efficient
                continue

            (item, got_task) = self.frontier.dequeue()
            if not got_task:
                time.sleep(0.1)
                continue

            url, parent, depth = item
            if CRAWL_DELAY > 0:
                time.sleep(CRAWL_DELAY)
                
            start = time.time()
            try:
                self.info(f"Crawling {url}")
                fetch_url = force_www_url(url)
                result = fetch(fetch_url, parent, depth, siteid=self.siteid)
                fetched_at = datetime.now()

                if not result["success"]:
                    err = result.get("error", "unknown")
                    
                    # Suppress noisy messages for ignored content types (e.g., images)
                    if isinstance(err, str) and "ignored content type" in err:
                        with self.skip_lock:
                            self.skip_report["FETCH_IGNORED_CONTENT_TYPE"]["count"] += 1
                            if len(self.skip_report["FETCH_IGNORED_CONTENT_TYPE"]["urls"]) < 5:
                                self.skip_report["FETCH_IGNORED_CONTENT_TYPE"]["urls"].append(url)
                        continue

                    # Handle soft redirects on 404
                    if "http error: 404" in str(err).lower() and result.get("html"):
                        if self.is_soft_redirect(result["html"]):
                            cached = get_cached_render(url)
                            if cached:
                                html = cached
                                final_url = url # fallback
                            else:
                                html, final_url = JS_RENDERER.render(url)
                                if final_url:
                                    set_cached_render(final_url, html)

                            if final_url and final_url != url:
                                self.info(f"Soft Redirect recovered {url} -> {final_url}")
                                result["success"] = True
                                result["response"] = type('obj', (object,), {
                                    'status_code': 200, 
                                    'headers': {'Content-Type': 'text/html'}, 
                                    'text': html, 
                                    'content': html.encode()
                                })
                                result["final_url"] = final_url
                    
                    if not result["success"]:
                        self.error(f"Fetch failed for {url}: {err}")
                        if "429" in str(err):
                            self.failed_429_count += 1
                        self.failed_count += 1
                        continue

                resp = result["response"]
                ct = resp.headers.get("Content-Type", "").lower()
                final_url = result.get("final_url", url)

                if final_url != url:
                    self.info(f"[REDIRECT] {url} -> {final_url}")

                db_result = insert_crawl_page({
                    "job_id": self.job_id,
                    "custid": self.custid,
                    "siteid": self.siteid,
                    "url": self._db_url(final_url), # 🔒 Use final destination URL
                    "parent_url": self._db_url(parent) if parent else None,
                    "depth": depth,
                    "status_code": resp.status_code,
                    "content_type": ct,
                    "content_length": len(resp.content),
                    "response_time_ms": int((time.time() - start) * 1000),
                    "fetched_at": fetched_at,
                    "base_url": self.seed_url,
                })

                if not db_result:
                    self.policy_skipped_count += 1
                    continue

                db_action = db_result["action"]
                affected_id = db_result["id"]

                if db_action == "Inserted":
                    self.saved_count += 1
                    self.info(f"DB: Inserted {url} (ID: {affected_id})")
                elif db_action == "Existed":
                    self.existed_count += 1
                    self.info(f"DB: Existed (Not-Touched) {url} (ID: {affected_id})")
                
                if "text/html" not in ct:
                    continue

                # Optimization: Explicitly decode as utf-8 (ignoring errors) to avoid slow 
                # automatic encoding detection in requests (especially on large HTML like Cricbuzz).
                html = resp.content.decode('utf-8', errors='ignore')
                
                if self.target_urls:
                    urls = []
                else:
                    urls, _ = extract_urls(html, final_url)

                if not urls and needs_js_rendering(html):
                    cached = get_cached_render(final_url)
                    if cached:
                        self.info(f"JS: Using cached render for {final_url}")
                        html = cached
                    else:
                        self.info(f"JS: Switching to JS rendering for {final_url}")
                        html, rendered_url = JS_RENDERER.render(final_url)
                        if rendered_url:
                            final_url = rendered_url
                        set_cached_render(final_url, html)
                    
                    if not self.target_urls:
                        urls, _ = extract_urls(html, final_url)

                # ---------------- MODE LOGIC ----------------
                if self.crawl_mode == "BASELINE":
                    baseline_id, path, action = save_baseline(
                        custid=self.custid,
                        siteid=self.siteid,
                        url=self._db_url(url),
                        html=html,
                        base_url=self.seed_url,
                    )
                    self.info(f"DB: {action.upper()} baseline {baseline_id}")

                elif self.crawl_mode == "COMPARE":
                    results = self.compare_engine.handle_page(
                        siteid=self.siteid,
                        url=self._db_url(url),
                        html=html,
                    )
                    
                    if results and self.compare_results is not None:
                        # Append to global results
                        if self.compare_lock:
                            with self.compare_lock:
                                self.compare_results.extend(results)
                        else:
                            self.compare_results.extend(results)

                enqueued = 0
                skipped_rule = 0
                skipped_domain = 0

                for u in urls:
                    skip_type = classify_skip(u)
                    if skip_type:
                        with self.skip_lock:
                            self.skip_report[skip_type]["count"] += 1
                            if len(self.skip_report[skip_type]["urls"]) < 5:
                                self.skip_report[skip_type]["urls"].append(u)
                        skipped_rule += 1
                        continue

                    if not _allowed_domain(self.original_site_url, u, current_url=final_url):
                        with self.skip_lock:
                            self.skip_report["DOMAIN_FILTER"]["count"] += 1
                            if len(self.skip_report["DOMAIN_FILTER"]["urls"]) < 5:
                                self.skip_report["DOMAIN_FILTER"]["urls"].append(u)
                        skipped_domain += 1
                        continue

                    if self.frontier.enqueue(u, final_url, depth + 1, preference_url=self.original_site_url):
                        enqueued += 1
                    else:
                        self.frontier_duplicate_count += 1

                if enqueued > 0:
                    self.info(f"Enqueued {enqueued} URLs")
                if skipped_rule or skipped_domain:
                    self.info(f"Skipped: rules={skipped_rule}, domain={skipped_domain}")

            except Exception as e:
                import traceback
                self.error(f"ERROR {url}: {e}\n{traceback.format_exc()}")
            finally:
                self.frontier.mark_visited(url, got_task=got_task, preference_url=self.original_site_url)

    def stop(self):
        self.running = False
