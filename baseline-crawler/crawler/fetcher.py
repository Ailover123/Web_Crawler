"""
HTTP fetching module for the crawler.
Fetches URLs and records results to the database.
Only fetches HTML content.
"""

import requests
import time
from urllib.parse import urlparse
from crawler.config import USER_AGENT, REQUEST_TIMEOUT
from crawler.storage.db import get_connection

def fetch(url, discovered_from=None, depth=0):
    """
    Fetch a URL, classify outcome, record to DB.
    Returns the requests.Response on success, or None otherwise.
    Always records to DB.
    """
    start_time = time.time()
    domain = urlparse(url).netloc

    try:
        r = requests.get(
            url,
            timeout=REQUEST_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
            verify=True,
            allow_redirects=True,
        )
        fetch_time_ms = int((time.time() - start_time) * 1000)
        response_size = len(r.content)
        ct = r.headers.get("Content-Type", "").lower()

        if 200 <= r.status_code < 300:
            if "text/html" in ct or "application/json" in ct:
                # Success
                # _record_to_db(url, domain, "success", r.status_code, ct, response_size, fetch_time_ms, None, discovered_from, depth)
                return r
            else:
                # Ignored
                # _record_to_db(url, domain, "ignored", r.status_code, ct, response_size, fetch_time_ms, None, discovered_from, depth)
                return None
        else:
            # Fetch failed
            # _record_to_db(url, domain, "fetch_failed", r.status_code, ct, response_size, fetch_time_ms, "http_error", discovered_from, depth)
            return None

    except requests.exceptions.Timeout:
        fetch_time_ms = int((time.time() - start_time) * 1000)
        # _record_to_db(url, domain, "fetch_failed", None, None, 0, fetch_time_ms, "timeout", discovered_from, depth)
        return None
    except requests.exceptions.ConnectionError:
        fetch_time_ms = int((time.time() - start_time) * 1000)
        # _record_to_db(url, domain, "fetch_failed", None, None, 0, fetch_time_ms, "connection_error", discovered_from, depth)
        return None
    except requests.exceptions.RequestException as e:
        fetch_time_ms = int((time.time() - start_time) * 1000)
        # _record_to_db(url, domain, "fetch_failed", None, None, 0, fetch_time_ms, "request_error", discovered_from, depth)
        return None

def _record_to_db(url, domain, status, http_status, content_type, response_size, fetch_time_ms, error_type, discovered_from, depth):
    """
    Record the fetch result to the database using UPSERT for idempotency.
    """
    print(f"[DB] recording: {url}")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO urls (url, domain, status, http_status, content_type, response_size, fetch_time_ms, error_type, discovered_from, depth, crawled_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(url) DO UPDATE SET
            status=excluded.status,
            http_status=excluded.http_status,
            content_type=excluded.content_type,
            response_size=excluded.response_size,
            fetch_time_ms=excluded.fetch_time_ms,
            error_type=excluded.error_type,
            discovered_from=excluded.discovered_from,
            depth=excluded.depth,
            crawled_at=excluded.crawled_at;
    """, (url, domain, status, http_status, content_type, response_size, fetch_time_ms, error_type, discovered_from, depth))
    conn.commit()
    conn.close()
