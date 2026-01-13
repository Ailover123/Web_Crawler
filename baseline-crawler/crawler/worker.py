import time
import requests
from datetime import datetime
from typing import Optional, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from crawler.models import CrawlTask, CrawlArtifact
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

    def execute(self, task: CrawlTask) -> None:
        """
        Processes a single CrawlTask and writes a CrawlArtifact.
        Ensures an artifact is emitted even on failure.
        """
        # INVARIANT: Session is created per task to maintain statelessness.
        # Performance tradeoff is accepted for architectural purity.
        session = requests.Session()
        session.max_redirects = self._max_redirects
        
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

        headers = {}
        if task.user_agent:
            headers["User-Agent"] = task.user_agent

        request_timestamp = datetime.utcnow().isoformat()
        start_time = time.perf_counter()
        
        try:
            # INVARIANT: Follow redirects internally to record resolved_url.
            response = session.request(
                method=task.request_method,
                url=task.normalized_url,
                headers=headers,
                proxies=task.proxy_config,
                timeout=task.timeout_ms / 1000.0,
                allow_redirects=True,
                stream=False
            )
            
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            
            # INVARIANT: Content-Type fallback
            content_type = response.headers.get("Content-Type", "application/octet-stream")
            
            artifact = CrawlArtifact(
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
            # Emit a failure artifact for terminal network errors.
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            
            artifact = CrawlArtifact(
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

        # INVARIANT: Every attempt results in exactly one artifact.
        self._writer.write(artifact)
