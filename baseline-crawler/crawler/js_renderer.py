"""
Synchronous JS renderer using Playwright.
Uses a DEDICATED THREAD to handle all Playwright operations, avoiding
greenlet/thread-switching errors when called from ThreadPoolExecutor workers.
"""

import threading
import queue
from playwright.sync_api import sync_playwright
from crawler.config import (
    JS_GOTO_TIMEOUT,
    JS_WAIT_TIMEOUT,
    JS_STABILITY_TIME,
)
from crawler.logger import logger

# ------------------------------------------------------------
# Internal Protocol
# ------------------------------------------------------------
_request_queue = queue.Queue()
_init_lock = threading.Lock()
_worker_thread = None

class RenderRequest:
    def __init__(self, url):
        self.url = url
        self.result_queue = queue.Queue()

class RenderResult:
    def __init__(self, content=None, final_url=None, error=None):
        self.content = content
        self.final_url = final_url
        self.error = error

# ------------------------------------------------------------
# The Render Thread Loop
# ------------------------------------------------------------
def _render_loop():
    """
    Runs in a dedicated thread. Owns the Playwright instance.
    """
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            # Create a reusable context (could also create per-request for better isolation)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0 Safari/537.36"
                )
            )
            
            logger.info("[JS-RENDER] Dedicated render thread started.")

            while True:
                req = _request_queue.get()
                if req is None: # Poison pill
                    break
                
                try:
                    # PROCESS REQUEST
                    page = context.new_page()
                    try:
                        page.goto(req.url, wait_until="domcontentloaded", timeout=JS_GOTO_TIMEOUT * 1000)

                        # Wait for hydration
                        try:
                            page.wait_for_function(
                                "() => document.body && document.body.children.length > 0",
                                timeout=JS_WAIT_TIMEOUT * 1000,
                            )
                        except Exception:
                            pass

                        # Stability wait
                        if JS_STABILITY_TIME > 0:
                            page.wait_for_timeout(JS_STABILITY_TIME * 1000)

                        # Load state check
                        try:
                            page.wait_for_load_state("load", timeout=5000)
                        except Exception:
                            pass

                        final_url = page.url
                        content = page.content()
                        
                        req.result_queue.put(RenderResult(content, final_url))
                    
                    except Exception as e:
                        req.result_queue.put(RenderResult(error=e))
                    finally:
                        page.close()
                        
                except Exception as e:
                     logger.error(f"[JS-RENDER] Loop error: {e}")
                     # Ensure we always reply to unblock caller
                     # (Though if we are here, we might not have the req object or queue)
                     pass

    except Exception as e:
        logger.critical(f"[JS-RENDER] Fatal thread error: {e}")


def _ensure_worker_running():
    global _worker_thread
    if _worker_thread and _worker_thread.is_alive():
        return

    with _init_lock:
        if _worker_thread and _worker_thread.is_alive():
            return
        
        _worker_thread = threading.Thread(target=_render_loop, daemon=True, name="RenderWorker")
        _worker_thread.start()


# ------------------------------------------------------------
# Public API (Blocking)
# ------------------------------------------------------------
def render_js_sync(url: str) -> tuple[str, str]:
    """
    Prevents thread mismatch errors by delegating to the dedicated render thread.
    """
    _ensure_worker_running()

    req = RenderRequest(url)
    _request_queue.put(req)

    # BLOCK until result
    result = req.result_queue.get()

    if result.error:
        raise result.error
    
    return result.content, result.final_url
