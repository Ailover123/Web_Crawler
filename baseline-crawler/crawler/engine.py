"""
FILE DESCRIPTION: Global orchestration module managing worker threads, crawl frontier, and execution lifecycle.
CONSOLIDATED FROM: worker.py, frontier.py, queue.py
KEY FUNCTIONS/CLASSES: CrawlerWorker, Frontier, CrawlQueue, ExecutionPolicy
"""

import threading
import time
import re
from collections import defaultdict, deque
from urllib.parse import urlparse
from datetime import datetime
from queue import Queue, Empty

from crawler.core import CRAWL_DELAY, logger
from crawler.processor import LinkUtility, PageFetcher, LinkExtractor, TrafficControl
from crawler.js_engine import JSIntelligence, RenderCache, JSRenderWorker
from crawler.storage.db import insert_crawl_page
from crawler.storage.baseline_store import save_baseline
from crawler.compare_engine import CompareEngine

# === EXECUTION POLICY ===

class ExecutionPolicy:
    """
    FLOW: Defines static skip rules for URLs (Assets, Tags, Params) -> 
    Implements strict domain boundary checks -> Classifies if a URL should be enqueued.
    """
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
    @staticmethod
    def is_recursion(url):
        """Detects infinite recursion in paths (e.g. /a/b/a/b or repeated domain segments)"""
        parsed = urlparse(url)
        path = parsed.path.strip('/')
        if not path: return False
        segments = [s.lower() for s in path.split('/') if s]
        if len(segments) < 3: return False
        
        # 1. Simple count of identical segments
        counts = defaultdict(int)
        for s in segments:
            if len(s) < 3: continue
            counts[s] += 1
            if counts[s] > 2: return True
            
        # 2. Check for consecutive repeating sequences (e.g. /foo/bar/foo/bar)
        for n in range(1, len(segments)//2 + 1):
            for i in range(len(segments) - 2*n + 1):
                if segments[i:i+n] == segments[i+n:i+2*n]:
                    return True
        return False

    @classmethod
    def classify_skip(cls, url: str):
        parsed = urlparse(url)
        path = parsed.path or ""
        if path.lower().endswith(cls.STATIC_EXTENSIONS): return "STATIC"
        if parsed.query: return "QUERY_PARAM"
        for k, r in cls.PATH_SKIP_RULES.items():
            if re.search(r, path.lower()): return k
        for k, r in cls.QUERY_SKIP_RULES.items():
            if re.search(r, parsed.query.lower()): return k
        return None

    @staticmethod
    def is_allowed_domain(seed_url: str, candidate_url: str, current_url: str = None) -> bool:
        s_netloc = urlparse(seed_url).netloc.lower().split(":")[0]
        c_netloc = urlparse(candidate_url).netloc.lower().split(":")[0]
        if s_netloc == c_netloc: return True
        if current_url:
            curr_netloc = urlparse(current_url).netloc.lower().split(":")[0]
            if curr_netloc == c_netloc: return True
        s_base = s_netloc[4:] if s_netloc.startswith("www.") else s_netloc
        c_base = c_netloc[4:] if c_netloc.startswith("www.") else c_netloc
        return s_base == c_base


# === FRONTIER MANAGEMENT ===

class Frontier:
    """
    FLOW: Manages thread-safe discovery sets (visited, in-progress) -> 
    Implements duplicate prevention -> Maintains the primary work queue for worker threads.
    """
    def __init__(self):
        self.queue = Queue(maxsize=0)
        self.visited = set()
        self.in_progress = set()
        self.discovered = set()
        self.lock = threading.Lock()

    def enqueue(self, url, discovered_from=None, depth=0, preference_url=None) -> bool:
        normalized = LinkUtility.normalize_url(url, preference_url=preference_url)
        with self.lock:
            p = urlparse(url)
            if p.scheme in ("mailto", "tel", "javascript"): return False
            if normalized in self.visited or normalized in self.in_progress: return False
            
            # Recursion protection
            if ExecutionPolicy.is_recursion(normalized):
                return False

            self.in_progress.add(normalized)
            self.discovered.add(normalized)
        try:
            self.queue.put((normalized, discovered_from, depth))
            return True
        except Exception:
            with self.lock: self.in_progress.discard(normalized)
            return False

    def dequeue(self):
        try:
            return self.queue.get(timeout=0.5), True
        except Empty:
            return None, False

    def mark_visited(self, url, *, got_task: bool, preference_url=None):
        normalized = LinkUtility.normalize_url(url, preference_url=preference_url)
        with self.lock:
            self.in_progress.discard(normalized)
            self.visited.add(normalized)
        if got_task:
            try:
                self.queue.task_done()
            except ValueError:
                pass

    def get_stats(self):
        with self.lock:
            return {
                "queued": self.queue.qsize(),
                "visited_count": len(self.visited),
                "in_progress_count": len(self.in_progress),
                "discovered": len(self.discovered)
            }

# Legacy / Alternative Queue
class CrawlQueue:
    def __init__(self, max_depth):
        self.queue = deque()
        self.visited = set()
        self.queued = set()
        self.max_depth = max_depth

    def enqueue(self, url, depth):
        if depth > self.max_depth or url in self.visited or url in self.queued: return False
        self.queue.append((url, depth))
        self.queued.add(url)
        return True

    def dequeue(self):
        if not self.queue: return None
        url, depth = self.queue.popleft()
        self.queued.discard(url); self.visited.add(url)
        return url, depth


# === CRAWLER WORKER ===

JS_RENDERER = JSRenderWorker()

class CrawlerWorker(threading.Thread):
    """
    FLOW: Main worker loop -> Dequeues URL -> Throttles per domain rules -> 
    Fetches (with optional JS escalation) -> Persists to DB -> Extracts new links -> Re-enqueues per policy.
    """
    SKIP_REPORT = defaultdict(lambda: {"count": 0, "urls": []})
    SKIP_LOCK = threading.Lock()

    def __init__(self, frontier, name, custid, siteid_map, job_id, crawl_mode, seed_url, original_site_url=None, skip_report=None, skip_lock=None, target_urls=None, compare_results=None, compare_lock=None):
        super().__init__(name=name)
        self.frontier = frontier
        self.running = True
        self.custid = custid
        self.siteid = next(iter(siteid_map.values()))
        self.job_id = job_id
        self.crawl_mode = crawl_mode
        self.seed_url = seed_url
        self.original_site_url = original_site_url
        self.skip_report = skip_report if skip_report is not None else defaultdict(lambda: {"count": 0, "urls": []})
        self.skip_lock = skip_lock if skip_lock is not None else threading.Lock()
        self.target_urls = target_urls
        self.compare_results = compare_results
        self.compare_lock = compare_lock
        self.saved_count = 0
        self.failed_count = 0
        self.policy_skipped_count = 0
        self.frontier_duplicate_count = 0
        self.redirect_count = 0
        self.existed_urls = set()
        self.new_urls = []
        self.js_render_stats = {"total": 0, "success": 0, "failed": 0}
        self.failure_reasons = defaultdict(int)
        self.failed_throttle_count = 0 # Tracks 429 and 503 errors

    def is_soft_redirect(self, html: str) -> bool:
        if not html: return False
        h = html.lower()
        # Standard Meta/JS redirects
        if 'http-equiv="refresh"' in h or 'window.location' in h:
            return True
        # Sucuri Cloudproxy anti-bot challenge
        if 'sucuri_cloudproxy_js' in h or 'sucuri.net/using-firewall' in h:
            return True
        return False

    def _db_url(self, url):
        p = urlparse(url)
        host = p.netloc.lower()
        if self.original_site_url:
            temp_nl = urlparse(self.original_site_url if "://" in self.original_site_url else "https://"+self.original_site_url)
            nl = temp_nl.netloc.lower()
            
            if host.startswith("www.") and not nl.startswith("www."): 
                host = host[4:]
        return f"{host}{p.path or ''}{'?' + p.query if p.query else ''}"

    def log(self, level, msg):
        getattr(logger, level)(msg, extra={'context': self.name})

    def run(self):
        self.log("info", f"started ({self.crawl_mode})")
        while self.running:
            remaining = TrafficControl.get_remaining_pause(self.siteid)
            if remaining > 0:
                 time.sleep(remaining)

            item, found = self.frontier.dequeue()
            if not found:
                # If no URLs and no active threads, we are done
                if self.job_id and self.frontier.get_stats()["in_progress_count"] == 0:
                    break
                time.sleep(1)
                continue

            url, discovered_from, depth = item
            
            # double check visited (race condition in queue)
            if url in self.existed_urls:
                self.frontier.mark_visited(url, got_task=True, preference_url=self.original_site_url)
                continue

            try:
                # -------------------------------
                # 1. FETCH
                # -------------------------------
                
                # Check 429 backoff again accurately before fetch
                remaining = TrafficControl.get_remaining_pause(self.siteid)
                if remaining > 0:
                     time.sleep(remaining)

                if CRAWL_DELAY > 0:
                    time.sleep(CRAWL_DELAY)

                fetch_url = LinkUtility.force_www_url(url)
                result = PageFetcher.fetch(fetch_url, siteid=self.siteid, referer=discovered_from)
                
                if result.get("error") and any(err in str(result["error"]) for err in ("429", "503")):
                    self.failed_throttle_count += 1
                
                resp_obj = result.get("response")
                initial_status = resp_obj.status_code if resp_obj else 0

                # 🛡️ Detect Soft 404 in raw HTML
                if result["success"] and result.get("html"):
                    if JSIntelligence.is_404_content(result["html"]):
                        self.log("info", f"Soft 404 detected in raw HTML for {url}. Tracking as 404.")
                        result["success"] = False
                        initial_status = 404

                if not result["success"]:
                    # ONLY escalate if it's NOT a 404, OR if it's a known Sucuri/security challenge
                    is_404 = (initial_status == 404)
                    has_challenge = 'sucuri' in result.get("html", "").lower() or 'cloudproxy' in result.get("html", "").lower()
                    
                    if result.get("html") and self.is_soft_redirect(result["html"]) and (not is_404 or has_challenge):
                        self.log("info", f"Soft Redirect/Challenge detected for {url} (Status: {initial_status}). Escalating to JS Rendering...")
                        self.js_render_stats["total"] += 1
                        try:
                            html, final_url, js_status = JS_RENDERER.render(url)
                            self.js_render_stats["success"] += 1
                            
                            # 🛡️ Detect Soft 404 in RENDERED HTML
                            if JSIntelligence.is_404_content(html):
                                self.log("info", f"Soft 404 detected in RENDERED HTML for {url}. Tracking as 404.")
                                js_status = 404

                            if final_url and 200 <= js_status < 400:
                                result["success"] = True
                                result["final_url"] = final_url
                                # Fake a successful response object with the REAL status
                                result["response"] = type('obj', (object,), {
                                    'status_code': js_status,
                                    'headers': {'Content-Type': 'text/html'},
                                    'content': html.encode(),
                                    'text': html
                                })
                            else:
                                reason = f"JS render returned status {js_status}"
                                self.failed_count += 1
                                self.failure_reasons[reason] += 1
                                self.log("error", f"Fetch failed after JS escalation for {url}: {reason}")
                                continue
                        except Exception as js_err:
                            self.js_render_stats["failed"] += 1
                            self.log("error", f"JS Render escalation failed: {js_err}")

                    if not result["success"]:
                        self.failed_count += 1
                        reason = result.get("error", "Unknown Fetch Error")
                        if initial_status == 404: reason = "http error: 404"
                        self.failure_reasons[reason] += 1
                        self.log("error", f"Fetch failed for {url}: {reason}")
                        continue

                resp = result["response"]
                final_url = result.get("final_url", url)
                if final_url != url:
                    self.redirect_count += 1
                    self.log("info", f"[REDIRECT] {url} -> {final_url}")
                
                db_res = insert_crawl_page({
                    "job_id": self.job_id, "custid": self.custid, "siteid": self.siteid,
                    "url": self._db_url(final_url), "parent_url": self._db_url(discovered_from) if discovered_from else None,
                    "depth": depth, "status_code": resp.status_code, "content_type": resp.headers.get("Content-Type", ""),
                    "content_length": len(resp.content), "response_time_ms": result["fetch_time_ms"],
                    "fetched_at": datetime.now(), "base_url": self.original_site_url
                })
                if db_res:
                    action = db_res.get("action")
                    affected_id = db_res.get("id", "?")
                    if action == "Inserted":
                        self.saved_count += 1
                        self.new_urls.append(final_url)
                        self.log("info", f"DB: Inserted {final_url} (ID: {affected_id})")
                    elif action == "Existed":
                        self.existed_urls.add(final_url)
                        self.log("info", f"DB: Existed (Not-Touched) {final_url} (ID: {affected_id})")

                html = resp.content.decode('utf-8', errors='ignore')
                if not self.target_urls:
                    urls, _ = LinkExtractor.extract_urls(html, final_url)
                    if not urls and JSIntelligence.needs_js_rendering(html):
                        self.js_render_stats["total"] += 1
                        try:
                            html, final_url, js_status = JS_RENDERER.render(final_url)
                            self.js_render_stats["success"] += 1
                            urls, _ = LinkExtractor.extract_urls(html, final_url)
                        except Exception as e:
                            self.js_render_stats["failed"] += 1
                            self.log("error", f"JS Render failed: {e}")

                    if urls:
                        self.log("info", f"Enqueued {len(urls)} URLs")
                    
                    policy_skipped = 0
                    domain_skipped = 0
                    
                    for u in urls:
                        reason = ExecutionPolicy.classify_skip(u)
                        if reason:
                            policy_skipped += 1
                            self.policy_skipped_count += 1
                            with (self.skip_lock or threading.Lock()):
                                self.skip_report[reason]["count"] += 1
                                if len(self.skip_report[reason]["urls"]) < 100:
                                    self.skip_report[reason]["urls"].append(u)
                            continue
                            
                        if not ExecutionPolicy.is_allowed_domain(self.original_site_url, u, current_url=final_url):
                            domain_skipped += 1
                            self.policy_skipped_count += 1
                            with (self.skip_lock or threading.Lock()):
                                self.skip_report["OUT_OF_DOMAIN"]["count"] += 1
                            continue
                            
                        if not self.frontier.enqueue(u, final_url, depth + 1, preference_url=self.original_site_url):
                            self.frontier_duplicate_count += 1

                    if urls:
                        self.log("info", f"Skipped: rules={policy_skipped}, domain={domain_skipped}")

                if self.crawl_mode == "BASELINE":
                    b_id, b_path, b_action = save_baseline(custid=self.custid, siteid=self.siteid, url=self._db_url(url), html=html, base_url=self.seed_url)
                    self.log("info", f"DB: {b_action.upper()} baseline {b_id}")
                elif self.crawl_mode == "COMPARE":
                    engine = CompareEngine(custid=self.custid)
                    res = engine.handle_page(siteid=self.siteid, url=self._db_url(url), html=html)
                    if res and self.compare_results is not None:
                        with (self.compare_lock or threading.Lock()): self.compare_results.extend(res)

            except Exception as e:
                self.log("error", f"Process error for {url}: {e}")
            finally:
                self.frontier.mark_visited(url, got_task=found, preference_url=self.original_site_url)

    def stop(self):
        self.running = False
