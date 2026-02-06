"""
HTTP fetching module for the crawler.
Fetches URLs and returns structured results.
NO database writes happen here.
"""

import requests
import time
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from crawler.config import USER_AGENT, REQUEST_TIMEOUT
from crawler.logger import logger
from crawler.throttle import set_pause, get_remaining_pause


def fetch(url, discovered_from=None, depth=0, siteid=None):
    """
    Fetch a URL and return structured result.
    Includes exponential backoff for HTTP 429 (Rate Limit).
    Checks for global pauses before starting.
    """
    # --- GLOBAL PAUSE CHECK ---
    if siteid:
        remaining = get_remaining_pause(siteid)
        if remaining > 0:
            logger.info(f"[THROTTLE] Pre-fetch pause active for site {siteid}. Waiting {remaining:.1f}s...")
            time.sleep(remaining)

    max_retries = 3
    retry_delay = 5

    for attempt in range(max_retries + 1):
        if siteid and attempt > 0:
            remaining = get_remaining_pause(siteid)
            if remaining > 0:
                logger.info(f"[THROTTLE] Global pause detected during retry for site {siteid}. Waiting {remaining:.1f}s...")
                time.sleep(remaining)

        start_time = time.time()
        try:
            # More realistic browser headers to avoid 406/403
            headers = {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Cache-Control": "max-age=0",
            }
            r = requests.get(
                url,
                timeout=REQUEST_TIMEOUT,
                headers=headers,
                verify=False,
                allow_redirects=True,
            )

            fetch_time_ms = int((time.time() - start_time) * 1000)
            response_size = len(r.content)
            content_type = r.headers.get("Content-Type", "").lower()

            if r.status_code == 429:
                # Trigger global pause for all workers of this site immediately
                set_pause(siteid, 5)
                
                if attempt < max_retries:
                    logger.warning(f"[RETRY {attempt+1}/{max_retries}] 429 Rate Limit for {url}. Global 5s pause set. Waiting locally for {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    logger.info(f"Retrying {url} now (Attempt {attempt+2}/{max_retries+1})...")
                    continue
                else:
                    logger.error(f"429 Rate Limit persisted for {url} after {max_retries} retries. Final 5s pause.")
                    time.sleep(5)

            if 200 <= r.status_code < 300:
                if "text/html" in content_type or "application/json" in content_type:
                    return {
                        "success": True,
                        "response": r,
                        "final_url": r.url,
                        "fetch_time_ms": fetch_time_ms,
                        "response_size": response_size,
                        "content_type": content_type,
                    }
                else:
                    return {
                        "success": False,
                        "error": f"ignored content type: {content_type}",
                        "final_url": r.url,
                        "content_type": content_type,
                        "fetch_time_ms": fetch_time_ms,
                    }
            else:
                return {
                    "success": False,
                    "error": f"http error: {r.status_code}",
                    "response": r,
                    "final_url": r.url,
                    "content_type": content_type,
                    "fetch_time_ms": fetch_time_ms,
                    "html": r.text if "text/html" in content_type else "",
                }

        except requests.exceptions.Timeout:
            return {
                "success": False,
                "error": "timeout",
                "content_type": "",
                "fetch_time_ms": int((time.time() - start_time) * 1000),
            }

        except requests.exceptions.ConnectionError:
            if attempt < max_retries:
                logger.warning(f"[RETRY {attempt+1}/{max_retries}] Connection Error for {url}. Waiting {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay *= 2
                logger.info(f"Retrying {url} now (Attempt {attempt+2}/{max_retries+1})...")
                continue
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

