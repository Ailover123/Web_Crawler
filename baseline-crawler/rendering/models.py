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
    Invariant: Tied to exactly one CrawlArtifact via artifact_id.
    """
    rendered_artifact_id: str
    artifact_id: str # Link to crawl_artifacts.artifact_id
    rendered_body: Optional[str] = None
    status: RenderStatus
    js_error_log: List[str] = field(default_factory=list)
    render_duration_ms: int = 0
    rendering_version: str = "v1"
    render_timestamp: datetime = field(default_factory=datetime.utcnow)
