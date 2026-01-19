from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

class DetectionStatus(Enum):
    CLEAN = "CLEAN"
    POTENTIAL_DEFACEMENT = "POTENTIAL_DEFACEMENT"
    DEFACED = "DEFACED"
    FAILED = "FAILED"

class DetectionSeverity(Enum):
    NONE = "NONE"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

@dataclass(frozen=True)
class DetectionVerdict:
    """
    Immutable result of a defacement analysis operation.
    Phase 5 Invariant: Comparison between two PageVersions.
    """
    verdict_id: str
    session_id: str
    
    # Context
    url_hash: str
    
    # The Comparison
    previous_baseline_version_id: str
    current_page_version_id: str
    
    # Verdict Data
    status: DetectionStatus
    severity: DetectionSeverity
    confidence: float
    structural_drift: float
    content_drift: float
    detected_indicators: List[str]
    analysis_timestamp: datetime = field(default_factory=datetime.utcnow)
