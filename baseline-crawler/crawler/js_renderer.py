"""
Synchronous JS renderer using Playwright.
Designed for threaded crawlers with thread-safe rendering.
"""

import threading
from playwright.sync_api import sync_playwright

_playwright = None
_browser = None
_lock = threading.Lock()


def _ensure_browser():
    """Initialize Playwright browser instance (thread-safe)"""
    global _playwright, _browser

    if _browser:
        return

    with _lock:
        if _browser:
            return

        _playwright = sync_playwright().start()
        _browser = _playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )


def render_js_sync(url: str) -> str:
    """
    Render a URL using Playwright and return rendered HTML.
    Thread-safe: uses lock to prevent concurrent rendering issues.
    Blocks the calling thread briefly.
    """
    _ensure_browser()

    # Lock the entire rendering process to prevent greenlet threading issues
    with _lock:
        context = _browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        )
        page = context.new_page()

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
            context.close()
