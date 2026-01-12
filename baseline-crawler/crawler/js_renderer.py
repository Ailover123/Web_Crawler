"""
Synchronous JS renderer using Playwright.
Designed for threaded crawlers (NO async in workers).
"""

import threading
from playwright.sync_api import sync_playwright

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


def render_js_sync(url: str) -> str:
    """
    Render a URL using Playwright and return rendered HTML.
    Blocks the calling thread briefly.
    """

    _ensure_browser()

    page = _context.new_page()

    try:
        page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=8000,
        )

        # Wait ONLY until meaningful content exists
        try:
            page.wait_for_function(
                "() => document.body && document.body.innerText.length > 200",
                timeout=3000,
            )
        except Exception:
            # Don't fail if condition not met
            pass

        return page.content()

    finally:
        page.close()
