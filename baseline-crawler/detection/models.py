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
    Phase 5 Invariant: Deterministic linkage to Baseline and Artifact.
    """
    verdict_id: str
    artifact_id: str # Link to crawl_artifacts.artifact_id
    baseline_id: str # Link to site_baselines.baseline_id
    status: DetectionStatus
    severity: DetectionSeverity
    confidence: float
    structural_drift: float
    content_drift: float
    detected_indicators: List[str]
    analysis_timestamp: datetime = field(default_factory=datetime.utcnow)
