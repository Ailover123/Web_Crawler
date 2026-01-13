from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any

@dataclass(frozen=True)
class BaselineProfile:
    """
    Data model for Phase 4: Baseline Generation.
    Represents an immutable ground-truth profile of a site's structure and content.
    """
    normalized_url: str
    baseline_id: str
    structural_digest: str
    structural_features: Dict[str, int] # Tag counts for continuous drift
    content_features: Dict[str, Any]
    extraction_version: str
    created_at: datetime = field(default_factory=datetime.utcnow)
