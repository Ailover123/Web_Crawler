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
class CrawlResponse:
    """
    Output of Phase 1: Crawl & Fetch.
    Represents the raw network response in MEMORY.
    
    INVARIANT: This object is TRANSIENT.
    It is passed to Phase 2 (Normalization) and then discarded.
    The 'raw_body' is NEVER persisted to the database.
    """
    session_id: str
    crawl_task_id: str
    attempt_number: int
    normalized_url: str
    resolved_url: str
    
    # Payload (Transient)
    raw_body: bytes
    
    # Metadata
    http_status: int
    content_type: str
    response_headers: Dict[str, str]
    request_timestamp: str  # ISO8601
    fetch_duration_ms: int
    remote_address: Optional[str] = None
