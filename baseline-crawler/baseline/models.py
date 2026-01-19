from dataclasses import dataclass, field
from datetime import datetime

@dataclass(frozen=True)
class SiteBaseline:
    """
    Phase 4 Model: A promoted PageVersion that serves as the Source of Truth.
    """
    baseline_id: str
    site_id: int
    page_version_id: str
    is_active: bool
    promoted_at: datetime = field(default_factory=datetime.utcnow)
