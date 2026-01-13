import hashlib
import re
from datetime import datetime
from typing import Tuple, Optional, Dict, Any, Union
from enum import Enum

from crawler.models import CrawlArtifact
from baseline.models import BaselineProfile

class BaselineFailure(Enum):
    PROCESS_FAILED = "PROCESS_FAILED"
    EMPTY_CONTENT = "EMPTY_CONTENT"
    INELIGIBLE_STATUS = "INELIGIBLE_STATUS"

class BaselineExtractor:
    """
    Implementation of Phase 4: Baseline Generation.
    Invariants:
    - Eligibility: 200 <= http_status < 300.
    - Determinism: Artifact + Version = Same Profile.
    - Isolated: No external Lookups.
    """

    def __init__(self, extraction_version: str):
        self._version = extraction_version

    def generate(self, artifact: CrawlArtifact) -> Union[BaselineProfile, BaselineFailure]:
        """
        Processes a CrawlArtifact into a BaselineProfile.
        Returns BaselineProfile on success, or BaselineFailure on error.
        """
        # INVARIANT: Eligibility Check (200-299)
        if not (200 <= artifact.http_status < 300):
            return BaselineFailure.INELIGIBLE_STATUS

        # INVARIANT: Empty Content Check
        if not artifact.raw_body:
            return BaselineFailure.EMPTY_CONTENT

        try:
            # Decode body (Best effort UTF-8)
            # NOTE: Decoding is best-effort UTF-8 with replacement to guarantee 
            # byte-stream determinism across different environments.
            content = artifact.raw_body.decode('utf-8', errors='replace')
            
            # FEATURE EXTRACTION: Structural Distillation
            # Responsibility: Remove unstable/dynamic elements before hashing
            structural_html = self._distill_structure(content)
            structural_digest = hashlib.sha256(structural_html.encode('utf-8')).hexdigest()

            # FEATURE EXTRACTION: Content Features
            features = self._extract_features(content)

            # INVARIANT: Deterministic Profile Creation.
            # Identity is derived from stable inputs to ensure re-runs match perfectly.
            id_seed = f"{artifact.normalized_url}|{structural_digest}|{self._version}"
            deterministic_id = hashlib.sha256(id_seed.encode('utf-8')).hexdigest()

            profile = BaselineProfile(
                normalized_url=artifact.normalized_url,
                baseline_id=deterministic_id,
                structural_digest=structural_digest,
                content_features=features,
                extraction_version=self._version,
                created_at=datetime.utcnow()
            )
            return profile

        except Exception:
            # FAILURE SEMANTIC: Signal process failure on crash or corrupt data
            # INVARIANT: No side-effects (logging handled by caller).
            return BaselineFailure.PROCESS_FAILED

    def _distill_structure(self, html: str) -> str:
        """
        Strips dynamic and content-heavy data to produce a stable 'skeleton'.
        - Removes Script/Style/Comments
        - Focuses on tag hierarchy.

        NOTE: Structural distillation is regex-based and intentionally lossy.
        It detects coarse structural changes (e.g., defacement injections),
        not semantic DOM equivalence. It ignores attribute order and malformed
        closing tag nuances.
        """
        # Remove comments
        html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)
        # Remove script and style blocks
        html = re.sub(r'<script.*?>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style.*?>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove all text nodes (retain only tag structure)
        # This creates a "Tag Skeleton"
        tags = re.findall(r'<([a-zA-Z0-9]+).*?>', html)
        return "".join(tags)

    def _extract_features(self, html: str) -> Dict[str, Any]:
        """
        Extracts stable metadata features.
        NOTE: These features (title, desc) are for similarity weighting in Detection.
        They are NOT used as hard identifiers for baseline validity.
        """
        features = {}
        
        # Extract Title
        title_match = re.search(r'<title>(.*?)</title>', html, flags=re.IGNORECASE | re.DOTALL)
        features["title"] = title_match.group(1).strip() if title_match else None
        
        # Extract Meta Description
        meta_desc = re.search(r'<meta name="description" content="(.*?)"', html, flags=re.IGNORECASE)
        features["meta_description"] = meta_desc.group(1) if meta_desc else None

        # Content fingerprint (optional fuzzy hash could go here later)
        # For now, we just track core structural metadata
        features["approx_size"] = len(html)
        
        return features
