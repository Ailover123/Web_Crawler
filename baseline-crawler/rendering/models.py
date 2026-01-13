from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
from enum import Enum

class RenderStatus(Enum):
    SUCCESS = "SUCCESS"
    RENDER_TIMEOUT = "RENDER_TIMEOUT"
    RENDER_FAILED = "RENDER_FAILED"
    INELIGIBLE_TYPE = "INELIGIBLE_TYPE"

@dataclass(frozen=True)
class RenderedArtifact:
    """
    Immutable post-JS DOM snapshot.
    Invariant: Tied to exactly one CrawlArtifact via parent_id and attempt.
    """
    rendered_artifact_id: str
    normalized_url: str
    parent_artifact_id: str
    attempt_number: int
    status: RenderStatus
    rendered_body: Optional[str] = None
    js_error_log: List[str] = field(default_factory=list)
    render_duration_ms: int = 0
    rendering_version: str = "v1"
    render_timestamp: datetime = field(default_factory=datetime.utcnow)
