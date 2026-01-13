from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any

@dataclass(frozen=True)
class BaselineProfile:
    """
    Data model for Phase 4: Baseline Generation.
    Represents an immutable ground-truth profile linked to a global site baseline.
    """
    profile_id: str # Deterministic: sha256(baseline_id + normalized_url)
    baseline_id: str # Link to site_baselines.baseline_id
    normalized_url: str
    structural_digest: str
    structural_features: Dict[str, int]
    content_features: Dict[str, Any]
    created_at: datetime = field(default_factory=datetime.utcnow)
