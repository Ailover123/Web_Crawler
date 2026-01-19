import time
import hashlib
import requests
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from crawler.config import REQUEST_TIMEOUT, USER_AGENT, VERIFY_SSL_CERTIFICATE
from crawler.models import CrawlTask, CrawlResponse
from crawler.storage import ArtifactWriter

class CrawlWorker:
    """
    Implementation of the Crawl Phase worker.
    Strictly follows forensic integrity rules: one execution per attempt.
    """

    def __init__(self, writer: ArtifactWriter):
        self._writer = writer
        # Redirect limit is fixed per architecture constraint
        self._max_redirects = 10 

    def _get_remote_address(self, response: requests.Response) -> Optional[str]:
        """
        Best-effort metadata extraction for remote host IP.
        Encapsulated to prevent implementation leaks or crashes.
        """
        try:
            # Note: This is implementation-specific (urllib3 + requests) 
            # and might not be available in all environments/protocols.
            raw_conn = response.raw._original_response.fp.raw._sock
            return raw_conn.getpeername()[0]
        except Exception:
            return None

    def execute(self, task: CrawlTask) -> Tuple[CrawlResponse, List[str]]:
        """
        Processes a single CrawlTask and writes a CrawlResponse log.
        Ensures a response is emitted even on failure.
        Returns: (response, extracted_urls)
        """
        # INVARIANT: Session is created per task to maintain statelessness.
        # Performance tradeoff is accepted for architectural purity.
        session = requests.Session()
        session.max_redirects = self._max_redirects
        session.verify = VERIFY_SSL_CERTIFICATE
        
        # VIOLATION FIX: Retries are connection-level only. 
        # Status codes 4xx/5xx must NOT be retried to preserve attempt semantics.
        retry_strategy = Retry(
            total=task.connection_retry_limit,
            connect=task.connection_retry_limit,
            read=task.connection_retry_limit,
            status=0,
            status_forcelist=[],
            allowed_methods=["GET", "HEAD"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        headers = {
            "User-Agent": USER_AGENT
        }

        request_timestamp = datetime.now(timezone.utc).isoformat()
        start_time = time.perf_counter()
        
        try:
            # INVARIANT: Follow redirects internally to record resolved_url.
            response = session.request(
                method=task.request_method,
                url=task.normalized_url,
                headers=headers,
                proxies=task.proxy_config,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
                stream=False
            )
            
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            
            # INVARIANT: Content-Type fallback
            content_type = response.headers.get("Content-Type", "application/octet-stream")
            
            session_id = task.crawl_task_id.split(':')[0]  # Extract from crawl_task_id
            
            crawl_response = CrawlResponse(
                session_id=session_id,
                crawl_task_id=task.crawl_task_id,
                attempt_number=task.attempt_number,
                normalized_url=task.normalized_url,
                resolved_url=response.url,
                raw_body=response.content,
                http_status=response.status_code,
                content_type=content_type,
                response_headers=dict(response.headers),
                request_timestamp=request_timestamp,
                fetch_duration_ms=duration_ms,
                remote_address=self._get_remote_address(response)
            )

        except (requests.exceptions.RequestException, Exception) as e:
            # VIOLATION FIX: Failure must be data, not absence.
            # Emit a failure response for terminal network errors.
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            
            session_id = task.crawl_task_id.split(':')[0]
            
            crawl_response = CrawlResponse(
                session_id=session_id,
                crawl_task_id=task.crawl_task_id,
                attempt_number=task.attempt_number,
                normalized_url=task.normalized_url,
                resolved_url=task.normalized_url, # Best guess on failure
                raw_body=b"",
                http_status=0, # Signal for terminal connection error
                content_type="application/octet-stream",
                response_headers={"error": str(e)},
                request_timestamp=request_timestamp,
                fetch_duration_ms=duration_ms,
                remote_address=None
            )

        # INVARIANT: Every attempt results in exactly one history log.
        self._writer.write(crawl_response)
        
        # Extract links from successful HTML responses
        extracted_urls = []
        if crawl_response.http_status == 200 and crawl_response.content_type and 'text/html' in crawl_response.content_type:
            extracted_urls = self._extract_links(crawl_response.raw_body, crawl_response.resolved_url)
        
        return crawl_response, extracted_urls
    
    def _extract_links(self, html_content: bytes, base_url: str) -> List[str]:
        """
        Extract all <a href> links from HTML and normalize to absolute URLs.
        Returns empty list on parse errors (fail-safe).
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            links = []
            
            for anchor in soup.find_all('a', href=True):
                href = anchor['href'].strip()
                if not href:
                    continue
                
                try:
                    # Normalize to absolute URL
                    absolute_url = urljoin(base_url, href)
                    links.append(absolute_url)
                except Exception:
                    # Ignore malformed URLs
                    continue
            
            return links
        except Exception:
            # Fail-safe: return empty list on parse errors
            return []
    
