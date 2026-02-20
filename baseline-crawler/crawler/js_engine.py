"""
FILE DESCRIPTION: Dedicated hub for all headless browser operations and JS rendering logic.
CONSOLIDATED FROM: js_detect.py, js_renderer.py, js_render_worker.py, render_cache.py
KEY FUNCTIONS/CLASSES: JSIntelligence, BrowserManager, JSRenderWorker, RenderCache
"""

import threading
import queue
import time
import hashlib
import re
from playwright.sync_api import sync_playwright
from crawler.core import (
    JS_GOTO_TIMEOUT,
    JS_WAIT_TIMEOUT,
    JS_STABILITY_TIME,
    USER_AGENT,
    logger
)

# === JS INTELLIGENCE ===

class JSIntelligence:
    """
    FLOW: Scans HTML content for known SPA markers (React/Vue/Next.js) -> 
    Checks if the body is a skeletal shell without semantic content -> Returns True if escalation to JS rendering is required.
    """
    @staticmethod
    def needs_js_rendering(html: str) -> bool:
        if not html:
            return True

        h = html.lower()

        # Explicit SPA roots
        if (
            '<div id="root"' in h or
            '<div id="app"' in h or
            '<app-root' in h or
            '<div id="__next"' in h
        ):
            return True

        # Body exists but is empty / shell
        if "<body" in h:
            body_start = h.find("<body")
            body = h[body_start:]

            # No real content indicators
            if (
                "<a " not in body and
                "<p" not in body and
                "<main" not in body and
                "<article" not in body and
                "<section" not in body
            ):
                return True

        return False

    @staticmethod
    def is_404_content(html: str) -> bool:
        """Heuristic to detect 404/Not Found content even if status is 200."""
        if not html: return False
        h = html.lower()
        # Common 404 patterns
        patterns = [
            "page not found",
            "404 not found",
            "404 - not found",
            "doesn't exist",
            "could not be found",
            "the page you're looking for",
            "error 404",
        ]
        # Check title first (stronger indicator)
        title_match = re.search(r'<title[^>]*>(.*?)</title>', h, re.IGNORECASE | re.DOTALL)
        if title_match:
            title_text = title_match.group(1).strip()
            if any(p in title_text for p in patterns) or "404" in title_text:
                return True

        # Check prominent headings/text
        if any(f"<{tag}" in h for tag in ["h1", "h2", "strong"]):
            # If any pattern appears and the page is very small, it's likely a 404
            if any(p in h for p in patterns) and len(h) < 5000:
                return True
        return False


# === RENDER CACHE ===

class RenderCache:
    """
    FLOW: Generates a hash key for a URL -> Checks in-memory dictionary for existing entries -> 
    Validates TTL (12h) -> Returns cached HTML or None.
    """
    CACHE_TTL_SECONDS = 60 * 60 * 12
    _cache = {}
    _lock = threading.Lock()

    @staticmethod
    def _cache_key(url: str) -> str:
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    @classmethod
    def get(cls, url: str):
        key = cls._cache_key(url)
        now = time.time()
        with cls._lock:
            entry = cls._cache.get(key)
            if not entry:
                return None
            html, ts = entry
            if now - ts > cls.CACHE_TTL_SECONDS:
                del cls._cache[key]
                return None
            return html

    @classmethod
    def set(cls, url: str, html: str):
        key = cls._cache_key(url)
        with cls._lock:
            cls._cache[key] = (html, time.time())


# === BROWSER MANAGER ===

class RenderRequest:
    def __init__(self, url):
        self.url = url
        self.result_queue = queue.Queue()

class RenderResult:
    def __init__(self, content=None, final_url=None, status_code=200, error=None):
        self.content = content
        self.final_url = final_url
        self.status_code = status_code
        self.error = error

class BrowserManager:
    """
    FLOW: Spawns multiple dedicated Playwright threads -> Each maintains its own independent browser context -> 
    Processes URLs from a shared queue -> Returns HTML + Final URL safely.
    """
    _request_queue = queue.Queue()
    _init_lock = threading.Lock()
    _worker_threads = []

    @classmethod
    def render_parallel(cls, url: str) -> tuple[str, str, int]:
        return cls.render_sync(url)

    @classmethod
    def _render_loop(cls, worker_id: int):
        """
        Independent worker loop. Each thread gets its own Playwright/Browser instance for thread-safety.
        """
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"]
                )
                context = browser.new_context(
                    user_agent=USER_AGENT,
                    viewport={"width": 1024, "height": 768}
                )
                logger.info(f"[JS-ENGINE] Render Worker-{worker_id} ready.")

                while True:
                    req = cls._request_queue.get()
                    if req is None: 
                        cls._request_queue.put(None) # Pass onto other workers
                        break
                    
                    try:
                        page = context.new_page()
                        try:
                            # RESOURCE BLOCKING
                            def route_intercept(route):
                                if route.request.resource_type in ["image", "font", "media"]:
                                    return route.abort()
                                return route.continue_()
                            page.route("**/*", route_intercept)

                            # Two-stage load for speed:
                            # 1. 'commit' to ensure site is reachable (Fast Fail)
                            try:
                                response = page.goto(req.url, wait_until="commit", timeout=20000)
                                status_code = response.status if response else 0
                                
                                # 2. If reachable, wait for content
                                if 200 <= status_code <= 299:
                                    try:
                                        page.wait_for_load_state("domcontentloaded", timeout=10000)
                                    except Exception: pass
                            except Exception as e:
                                req.result_queue.put(RenderResult(error=e))
                                continue

                            content = page.content()
                            req.result_queue.put(RenderResult(content, page.url, status_code))
                        except Exception as e:
                            req.result_queue.put(RenderResult(error=e))
                        finally:
                            page.close()
                    except Exception as e:
                        logger.error(f"[JS-ENGINE] Worker-{worker_id} error: {e}")
                
                browser.close()
        except Exception as e:
            logger.critical(f"[JS-ENGINE] Worker-{worker_id} fatal error: {e}")

    @classmethod
    def _ensure_running(cls):
        if cls._worker_threads and all(t.is_alive() for t in cls._worker_threads):
            return
        with cls._init_lock:
            if cls._worker_threads and all(t.is_alive() for t in cls._worker_threads):
                return
            cls._worker_threads = []
            num_workers = 5
            for i in range(num_workers):
                t = threading.Thread(target=cls._render_loop, args=(i,), daemon=True, name=f"RenderWorker-{i}")
                t.start()
                cls._worker_threads.append(t)

    @classmethod
    def render_sync(cls, url: str) -> tuple[str, str, int]:
        cls._ensure_running()
        req = RenderRequest(url)
        cls._request_queue.put(req)
        result = req.result_queue.get()
        if result.error:
            raise result.error
        return result.content, result.final_url, result.status_code

    @staticmethod
    def normalize_rendered_html(html: str) -> str:
        """Internal helper for basic rendering cleanup."""
        if not html: return ""
        if "\\n" in html: html = html.replace("\\n", "\n")
        return html.strip()


# === JS RENDER WORKER (THREADED API) ===

class JSRenderWorker(threading.Thread):
    """
    FLOW: Acts as a middleware thread -> Receives render requests -> Calls BrowserManager for actual rendering -> 
    Applies normalization -> Signals completion via a threading.Event.
    """
    def __init__(self):
        super().__init__(daemon=True)
        self.queue = queue.Queue()
        self.start()

    def run(self):
        while True:
            url, result_event = self.queue.get()
            try:
                html, final_url, status_code = BrowserManager.render_sync(url)
                result_event["html"] = BrowserManager.normalize_rendered_html(html)
                result_event["final_url"] = final_url
                result_event["status_code"] = status_code
            except Exception as e:
                result_event["error"] = e
            finally:
                result_event["done"].set()
                self.queue.task_done()

    def render(self, url: str, timeout: int = 30) -> tuple[str, str, int]:
        event = {"done": threading.Event(), "html": None, "final_url": None, "status_code": 200, "error": None}
        self.queue.put((url, event))
        if not event["done"].wait(timeout=timeout):
            raise TimeoutError(f"JS rendering timed out after {timeout}s for {url}")
        if event["error"]:
            raise event["error"]
        return event["html"], event["final_url"], event["status_code"]
