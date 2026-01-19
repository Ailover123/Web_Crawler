from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

@dataclass(frozen=True)
class PageVersion:
    """
    Output of Phase 2: Normalization.
    The Single Source of Truth for page content.
    """
    page_version_id: str      # Deterministic Hash(url + content_hash + version)
    url_hash: str             # SHA256(normalized_url)
    content_hash: str         # SHA256(normalized_text)
    
    # Content (Clean Text)
    title: Optional[str]
    normalized_text: str
    
    # Metadata
    normalized_url: str
    normalization_version: str = "v1"
    created_at: datetime = field(default_factory=datetime.utcnow)
