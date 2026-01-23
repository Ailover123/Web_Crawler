"""
Synchronous JS renderer using Playwright.
Designed for threaded crawlers (NO async in workers).
"""

import threading
from playwright.sync_api import sync_playwright
from crawler.config import (
    JS_GOTO_TIMEOUT,
    JS_WAIT_TIMEOUT,
    JS_STABILITY_TIME,
)

_browser = None
_context = None
_lock = threading.Lock()


def _ensure_browser():
    global _browser, _context

    if _browser and _context:
        return

    with _lock:
        if _browser and _context:
            return

        p = sync_playwright().start()
        _browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        _context = _browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        )


def render_js_sync(url: str) -> tuple[str, str]:
    """
    Render a URL using Playwright and return (rendered_html, final_url).
    Blocks the calling thread briefly.
    """
    _ensure_browser()

    page = _context.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=JS_GOTO_TIMEOUT * 1000)

        # Wait for React/Vue/Angular hydration
        try:
            page.wait_for_function(
                "() => document.body && document.body.children.length > 0",
                timeout=JS_WAIT_TIMEOUT * 1000,
            )
        except Exception:
            pass

        # Extra micro-wait for React commit phase or delayed redirects
        if JS_STABILITY_TIME > 0:
            page.wait_for_timeout(JS_STABILITY_TIME * 1000)

        # Robust content retrieval: if navigation is in progress, wait for it
        try:
            # Check if we landed on a new URL and wait for it to load
            page.wait_for_load_state("load", timeout=5000)
        except Exception:
            pass

        final_url = page.url
        try:
            content = page.content()
        except Exception:
            # Fallback if content is still locked
            page.wait_for_timeout(1000)
            content = page.content()
            final_url = page.url

        return content, final_url
    finally:
        page.close()
