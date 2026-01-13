from dataclasses import dataclass, field
from typing import Dict, Optional, Any
from datetime import datetime

@dataclass(frozen=True)
class CrawlTask:
    """
    Input schema for the Crawl Phase.
    Stable identifier and metadata owned by the Frontier.
    """
    crawl_task_id: str
    attempt_number: int
    normalized_url: str
    request_method: str = "GET"
    timeout_ms: int = 30000
    connection_retry_limit: int = 3
    user_agent: Optional[str] = None
    proxy_config: Optional[Dict[str, Any]] = None

@dataclass(frozen=True)
class CrawlArtifact:
    """
    Output schema for the Crawl Phase.
    Represents exact capture of the network response.
    """
    crawl_task_id: str
    attempt_number: int
    normalized_url: str
    resolved_url: str
    raw_body: bytes
    http_status: int
    content_type: str
    response_headers: Dict[str, str]
    request_timestamp: str  # ISO8601
    fetch_duration_ms: int
    remote_address: Optional[str] = None
