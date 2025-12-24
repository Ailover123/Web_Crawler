"""Fetch utilities for the crawler.

This module intentionally keeps a stable contract: callers receive either a
``requests.Response`` for successful HTML/JSON responses, or a plain `dict`
with ``ok: False`` and diagnostic fields. This avoids AttributeError in
callers that expect a Response object.
"""

import requests
import time
from crawler.config import USER_AGENT, REQUEST_TIMEOUT, REQUEST_DELAY


def fetch(url):
    """Fetch a URL and return a Response or a failure dict.

    Returns:
      - ``requests.Response`` on success (only for content types containing
        ``html`` or ``json``)
      - ``dict`` with ``ok: False`` and diagnostics on failure
    """
    try:
        r = requests.get(
            url,
            timeout=REQUEST_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
            verify=True,
            allow_redirects=True,
        )
    except requests.exceptions.RequestException as e:
        return {"ok": False, "url": url, "error": "request_exception", "message": str(e)}

    # Check HTTP status
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        ct = r.headers.get("Content-Type", "").lower()
        return {"ok": False, "url": url, "error": "http_error", "status_code": r.status_code, "content_type": ct, "message": str(e)}

    # Accept only HTML or JSON for crawling
    ct = r.headers.get("Content-Type", "").lower()
    if "html" in ct or "json" in ct:
        try:
            time.sleep(REQUEST_DELAY)
        except Exception:
            pass
        return r

    return {"ok": False, "url": url, "error": "unsupported_content_type", "status_code": r.status_code, "content_type": ct}
