"""
HTTP fetching module for the crawler.
Fetches URLs and records results to the database.
Only fetches HTML content.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
from urllib.parse import urlparse
from crawler.config import USER_AGENT, REQUEST_TIMEOUT
from crawler.storage.db import get_connection

# Shared session with retry/backoff to smooth over transient slowdowns without growing memory usage.
_session = requests.Session()
_retry = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET", "HEAD"),
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=10)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)

def fetch(url, discovered_from=None, depth=0):
    """
    Fetch a URL, classify outcome, record to DB.
    Returns a dict with 'success', 'response' or 'error'.
    """
    start_time = time.time()
    domain = urlparse(url).netloc

    try:
        print(f"[fetch] GET {url} with timeout={REQUEST_TIMEOUT}s and retries={_retry.total}")
        r = _session.get(
            url,
            timeout=(5, REQUEST_TIMEOUT),  # (connect_timeout, read_timeout)
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
                return {'success': True, 'response': r}
            else:
                # Skipped - assets/media that we don't crawl
                # _record_to_db(url, domain, "skipped", r.status_code, ct, response_size, fetch_time_ms, None, discovered_from, depth)
                return {'success': False, 'error': f'skipped: {ct}', 'status': 'skipped'}
        elif r.status_code == 404:
            # Not Found - page doesn't exist
            # _record_to_db(url, domain, "not_found", r.status_code, ct, response_size, fetch_time_ms, "not_found", discovered_from, depth)
            return {'success': False, 'error': 'not found (404)', 'status': 'not_found'}
        else:
            # Fetch failed - other HTTP errors
            # _record_to_db(url, domain, "fetch_failed", r.status_code, ct, response_size, fetch_time_ms, "http_error", discovered_from, depth)
            return {'success': False, 'error': f'http error: {r.status_code}', 'status': 'failed'}

    except requests.exceptions.Timeout:
        fetch_time_ms = int((time.time() - start_time) * 1000)
        # _record_to_db(url, domain, "fetch_failed", None, None, 0, fetch_time_ms, "timeout", discovered_from, depth)
        print(f"[fetch] timeout after {fetch_time_ms}ms: {url}")
        return {'success': False, 'error': 'timeout', 'status': 'failed'}
    except requests.exceptions.ConnectionError:
        fetch_time_ms = int((time.time() - start_time) * 1000)
        # _record_to_db(url, domain, "fetch_failed", None, None, 0, fetch_time_ms, "connection_error", discovered_from, depth)
        print(f"[fetch] connection error after {fetch_time_ms}ms: {url}")
        return {'success': False, 'error': 'connection error', 'status': 'failed'}
    except requests.exceptions.RequestException as e:
        fetch_time_ms = int((time.time() - start_time) * 1000)
        # _record_to_db(url, domain, "fetch_failed", None, None, 0, fetch_time_ms, "request_error", discovered_from, depth)
        print(f"[fetch] request error after {fetch_time_ms}ms: {url} -> {e}")
        return {'success': False, 'error': str(e), 'status': 'failed'}

def _record_to_db(url, domain, status, http_status, content_type, response_size, fetch_time_ms, error_type, discovered_from, depth):
    """
    Record the fetch result to the database using UPSERT for idempotency.
    """
    print(f"[DB] recording: {url}")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO urls (url, domain, status, http_status, content_type, response_size, fetch_time_ms, error_type, discovered_from, depth, crawled_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            status=VALUES(status),
            http_status=VALUES(http_status),
            content_type=VALUES(content_type),
            response_size=VALUES(response_size),
            fetch_time_ms=VALUES(fetch_time_ms),
            error_type=VALUES(error_type),
            discovered_from=VALUES(discovered_from),
            depth=VALUES(depth),
            crawled_at=VALUES(crawled_at);
    """, (url, domain, status, http_status, content_type, response_size, fetch_time_ms, error_type, discovered_from, depth))
    conn.commit()
    conn.close()
