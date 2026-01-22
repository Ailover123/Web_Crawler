"""
HTTP fetching module for the crawler.
Fetches URLs and returns structured results.
NO database writes happen here.
"""

import requests
import time
from crawler.config import USER_AGENT, REQUEST_TIMEOUT


def fetch(url, discovered_from=None, depth=0):
    """
    Fetch a URL and return structured result.
    Includes exponential backoff for HTTP 429 (Rate Limit).
    """
    max_retries = 2
    retry_delay = 2  # Start with 2 seconds

    for attempt in range(max_retries + 1):
        start_time = time.time()
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
            content_type = r.headers.get("Content-Type", "").lower()

            if r.status_code == 429 and attempt < max_retries:
                print(f"[RETRY {attempt+1}/{max_retries}] 429 Rate Limit for {url}. Waiting {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay *= 2
                continue

            if 200 <= r.status_code < 300:
                if "text/html" in content_type or "application/json" in content_type:
                    return {
                        "success": True,
                        "response": r,
                        "fetch_time_ms": fetch_time_ms,
                        "response_size": response_size,
                        "content_type": content_type,
                    }
                else:
                    return {
                        "success": False,
                        "error": f"ignored content type: {content_type}",
                        "content_type": content_type,
                        "fetch_time_ms": fetch_time_ms,
                    }
            else:
                return {
                    "success": False,
                    "error": f"http error: {r.status_code}",
                    "content_type": content_type,
                    "fetch_time_ms": fetch_time_ms,
                }

        except requests.exceptions.Timeout:
            return {
                "success": False,
                "error": "timeout",
                "content_type": "",
                "fetch_time_ms": int((time.time() - start_time) * 1000),
            }

        except requests.exceptions.ConnectionError:
            return {
                "success": False,
                "error": "connection error",
                "content_type": "",
                "fetch_time_ms": int((time.time() - start_time) * 1000),
            }

        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "error": str(e),
                "content_type": "",
                "fetch_time_ms": int((time.time() - start_time) * 1000),
            }

