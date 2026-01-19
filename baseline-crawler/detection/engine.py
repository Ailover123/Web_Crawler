import uuid
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from crawler.models import CrawlResponse
from baseline.models import BaselineProfile
from detection.models import DetectionVerdict, DetectionStatus, DetectionSeverity
import detection.extraction.v1 as extraction_v1

class DefacementDetector:
    """
    Implementation of Phase 5: Defacement Detection.
    A pure analytical comparison engine, version-isolated and deterministic.
    """

    def __init__(self, extraction_version: str = "v1"):
        # INVARIANT: Pinned to specific versioned extraction logic (v1).
        # We do NOT import phase 4 BaselineExtractor class.
        self._version = extraction_version

    def analyze(
        self, 
        artifact: CrawlResponse, 
        baseline: BaselineProfile, 
        policy: Optional[Dict[str, Any]] = None
    ) -> DetectionVerdict:
        """
        Calculates drift and classifies status from CrawlArtifact and BaselineProfile.
        - Checks Identity Invariant
        - Checks Version Parity
        - Computes Drifts (Continuous)
        - Classifies Outcome (Deterministic)
        """
        policy = policy or {}
        thresholds = policy.get("thresholds", {})
        
        # INVARIANT: Identity Mismatch Detection (Contract violation)
        if artifact.normalized_url != baseline.normalized_url:
            raise ValueError(f"Identity mismatch: {artifact.normalized_url} vs {baseline.normalized_url}")

        # INVARIANT: Incompatible logic check
        if baseline.extraction_version != self._version:
             return self._create_failed_verdict(artifact, baseline, "INCOMPATIBLE_VERSION")

        try:
            # 1. PARITY EXTRACTION (Pinned v1 logic only)
            content = artifact.raw_body.decode('utf-8', errors='replace')
            curr_data = extraction_v1.distill_v1_features(content)

            # 2. DRIFT CALCULATION
            # Structural drift: Continuous Jaccard Distance over tag bags.
            # INVARIANT: Deterministic formula documented in turn 356 response.
            s_drift = self._calculate_structural_drift_continuous(
                curr_data["structural_features"], 
                baseline.structural_features
            )
            
            # Content drift: Explicit mismatch ratio.
            c_drift = self._calculate_content_drift_explicit(
                curr_data["content_features"], 
                baseline.content_features
            )

            # 3. INDICATOR DETECTION
            indicators = self._detect_indicators(s_drift, c_drift, curr_data, baseline)

            # 4. DETERMINISTIC CLASSIFICATION
            status, severity, confidence = self._classify(s_drift, c_drift, indicators, thresholds)

            return DetectionVerdict(
                verdict_id=str(uuid.uuid4()),
                normalized_url=artifact.normalized_url,
                baseline_id=baseline.baseline_id,
                status=status,
                severity=severity,
                confidence=confidence,
                structural_drift=s_drift,
                content_drift=c_drift,
                detected_indicators=indicators,
                analysis_timestamp=datetime.utcnow()
            )

        except Exception:
            return self._create_failed_verdict(artifact, baseline, "PROCESS_FAILED")

    def _calculate_structural_drift_continuous(self, current: Dict[str, int], baseline: Dict[str, int]) -> float:
        """
        Continuous Jaccard-like distance for tag bags.
        distance = 1.0 - (Intersection_Size / Union_Size)
        """
        all_tags = set(current.keys()) | set(baseline.keys())
        if not all_tags:
            return 0.0
        
        intersection_size = 0
        union_size = 0
        for tag in all_tags:
            c = current.get(tag, 0)
            b = baseline.get(tag, 0)
            intersection_size += min(c, b)
            union_size += max(c, b)
            
        if union_size == 0:
            return 0.0
        
        return 1.0 - (float(intersection_size) / union_size)

    def _calculate_content_drift_explicit(self, current: Dict[str, Any], baseline: Dict[str, Any]) -> float:
        """
        Explicit Formula: (Mismatched Features) / (Total Features).
        Handles missing values explicitly by counting as mismatch.
        Result is clamped [0.0, 1.0].
        """
        all_keys = set(current.keys()) | set(baseline.keys())
        if not all_keys:
            return 0.0
        
        mismatched = 0
        for k in all_keys:
            if current.get(k) != baseline.get(k):
                mismatched += 1
        
        return float(mismatched) / len(all_keys)

    def _detect_indicators(self, s_drift, c_drift, curr_data, baseline) -> List[str]:
        indicators = []
        if s_drift > 0:
            indicators.append("STRUCTURAL_MUTATION")
        if s_drift > 0.5:
            indicators.append("MAJOR_STRUCTURAL_COLLAPSE")
        if curr_data["content_features"].get("title") != baseline.content_features.get("title"):
            indicators.append("TITLE_DEVIATION")
        return indicators

    def _classify(self, s_drift, c_drift, indicators, thresholds) -> Tuple[DetectionStatus, DetectionSeverity, float]:
        """
        Ordinal and Policy-driven classification.
        Confidence is strictly monotonic with drift.
        """
        # Thresholds from policy (defaults mapped to interface noise floor)
        s_threshold = thresholds.get("structural", 0.05)
        c_threshold = thresholds.get("content", 0.1)

        # 1. CLEAN: Under noise floor
        if s_drift < s_threshold and c_drift < c_threshold:
            conf = 1.0 - max(s_drift, c_drift)
            return DetectionStatus.CLEAN, DetectionSeverity.NONE, conf

        # 2. DEFACED (Primary signal: Structural)
        if s_drift > 0.8:
            return DetectionStatus.DEFACED, DetectionSeverity.CRITICAL, 1.0
        if s_drift > 0.3:
            return DetectionStatus.DEFACED, DetectionSeverity.HIGH, 0.9
        
        # 3. POTENTIAL (Secondary signal: Content)
        if c_drift > 0.6 or s_drift > 0.1:
            return DetectionStatus.POTENTIAL_DEFACEMENT, DetectionSeverity.MEDIUM, 0.7
        
        return DetectionStatus.POTENTIAL_DEFACEMENT, DetectionSeverity.LOW, 0.5

    def _create_failed_verdict(self, artifact, baseline, reason: str) -> DetectionVerdict:
        return DetectionVerdict(
            verdict_id=str(uuid.uuid4()),
            normalized_url=artifact.normalized_url,
            baseline_id=baseline.baseline_id,
            status=DetectionStatus.FAILED,
            severity=DetectionSeverity.NONE,
            confidence=0.0,
            structural_drift=1.0,
            content_drift=1.0,
            detected_indicators=[f"ERROR_{reason}"]
        )
