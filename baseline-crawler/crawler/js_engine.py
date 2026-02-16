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
    FLOW: Spawns a dedicated Playwright thread -> Maintains a single browser context -> 
    Processes URLs from an internal queue -> Normalizes whitespace in result -> Returns HTML + Final URL.
    """
    _request_queue = queue.Queue()
    _init_lock = threading.Lock()
    _worker_thread = None

    @classmethod
    def _render_loop(cls):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"],
                )
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                    viewport={"width": 1024, "height": 768}
                )
                logger.info("[JS-ENGINE] Dedicated render thread started.")

                while True:
                    req = cls._request_queue.get()
                    if req is None: break
                    
                    try:
                        page = context.new_page()
                        try:
                            response = page.goto(req.url, wait_until="domcontentloaded", timeout=JS_GOTO_TIMEOUT * 1000)
                            status_code = response.status if response else 200
                            
                            try:
                                page.wait_for_function("() => document.body && document.body.children.length > 0", timeout=JS_WAIT_TIMEOUT * 1000)
                            except Exception: pass
                            
                            if JS_STABILITY_TIME > 0:
                                page.wait_for_timeout(JS_STABILITY_TIME * 1000)
                                
                            try:
                                page.wait_for_load_state("load", timeout=5000)
                            except Exception: pass

                            final_url = page.url
                            content = page.content()
                            req.result_queue.put(RenderResult(content, final_url, status_code))
                        except Exception as e:
                            req.result_queue.put(RenderResult(error=e))
                        finally:
                            page.close()
                    except Exception as e:
                        logger.error(f"[JS-ENGINE] Loop error: {e}")
        except Exception as e:
            logger.critical(f"[JS-ENGINE] Fatal thread error: {e}")

    @classmethod
    def _ensure_running(cls):
        if cls._worker_thread and cls._worker_thread.is_alive():
            return
        with cls._init_lock:
            if cls._worker_thread and cls._worker_thread.is_alive():
                return
            cls._worker_thread = threading.Thread(target=cls._render_loop, daemon=True, name="RenderWorker")
            cls._worker_thread.start()

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
