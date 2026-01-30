"""
In-memory cache for JS-rendered pages.
Prevents repeated Playwright renders.
"""

import time
import threading
import hashlib

CACHE_TTL_SECONDS = 60 * 60 * 12  # 12 hours

_cache = {}
_lock = threading.Lock()


def _cache_key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def get_cached_render(url: str):
    key = _cache_key(url)
    now = time.time()

    with _lock:
        entry = _cache.get(key)
        if not entry:
            return None

        html, ts = entry
        if now - ts > CACHE_TTL_SECONDS:
            del _cache[key]
            return None

        return html


def set_cached_render(url: str, html: str):
    key = _cache_key(url)
    with _lock:
        _cache[key] = (html, time.time())
