import hashlib
import uuid
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from crawler.models import CrawlArtifact
from rendering.models import RenderedArtifact, RenderStatus

class RenderError(Exception):
    """Base rendering exception."""
    pass

class RenderTimeoutError(RenderError):
    """Raised when JS execution exceeds time limits."""
    pass

class RenderExecutionError(RenderError):
    """Raised on critical browser/script execution failures."""
    pass

class RenderingBackend(ABC):
    """
    Abstraction for the underlying browser driver.
    Contractual Requirements for Implementers:
    - MUST enforce global timeout provided in options.
    - MUST disable cross-origin network requests (unless allowlisted).
    - MUST prevent navigation beyond the initial URL.
    - MUST follow bounded recursion for lazy-loading.
    """
    @abstractmethod
    def render(self, html: str, url: str, options: Dict[str, Any]) -> str:
        """
        Execute JS and return final serialized DOM.
        Raises RenderTimeoutError or RenderExecutionError.
        """
        pass

class RenderingEngine:
    """
    Implementation of Phase 6: Rendering / JS Enrichment.
    Invariants:
    - Determinism: Identity is derived from inputs via SHA-256.
    - Contractual: No string-matching on exceptions.
    - Isolated: Backend handles lifecycle security.
    """

    def __init__(self, backend: RenderingBackend, version: str = "v1"):
        self._backend = backend
        self._version = version

    def enrich(self, artifact: CrawlArtifact, options: Optional[Dict[str, Any]] = None) -> RenderedArtifact:
        """
        Produces a post-JS snapshot from a raw CrawlArtifact.
        """
        options = options or {}
        start_time = datetime.utcnow()
        
        # ELIGIBILITY RULE: Status and Content-Type Gates
        if not (200 <= artifact.http_status < 300) or \
           not artifact.content_type or "text/html" not in artifact.content_type.lower():
             status = RenderStatus.INELIGIBLE_TYPE if (200 <= artifact.http_status < 300) else RenderStatus.RENDER_FAILED
             return self._create_failure(artifact, status, start_time)

        try:
            raw_html = artifact.raw_body.decode('utf-8', errors='replace')
            
            # RENDERING SEMANTICS: Execute in tool-agnostic backend
            rendered_dom = self._backend.render(
                raw_html, 
                artifact.normalized_url,
                options
            )

            end_time = datetime.utcnow()
            return self._create_success(artifact, rendered_dom, start_time, end_time)

        except RenderTimeoutError:
            # FAILURE SEMANTIC: Typed timeout surfacing
            return self._create_failure(artifact, RenderStatus.RENDER_TIMEOUT, start_time)
        except (RenderExecutionError, Exception):
            # FAILURE SEMANTIC: Generic execution failure
            return self._create_failure(artifact, RenderStatus.RENDER_FAILED, start_time)

    def _create_success(self, artifact: CrawlArtifact, dom: str, start: datetime, end: datetime) -> RenderedArtifact:
        """Produces a deterministic success artifact."""
        return RenderedArtifact(
            rendered_artifact_id=self._generate_deterministic_id(artifact),
            normalized_url=artifact.normalized_url,
            parent_artifact_id=artifact.crawl_task_id,
            attempt_number=artifact.attempt_number,
            status=RenderStatus.SUCCESS,
            rendered_body=dom,
            render_duration_ms=int((end - start).total_seconds() * 1000),
            rendering_version=self._version,
            render_timestamp=end
        )

    def _create_failure(self, artifact: CrawlArtifact, status: RenderStatus, start: datetime) -> RenderedArtifact:
        """Produces a deterministic failure artifact."""
        end = datetime.utcnow()
        return RenderedArtifact(
            rendered_artifact_id=self._generate_deterministic_id(artifact),
            normalized_url=artifact.normalized_url,
            parent_artifact_id=artifact.crawl_task_id,
            attempt_number=artifact.attempt_number,
            status=status,
            render_duration_ms=int((end - start).total_seconds() * 1000),
            rendering_version=self._version,
            render_timestamp=end
        )

    def _generate_deterministic_id(self, artifact: CrawlArtifact) -> str:
        """
        INVARIANT: Deterministic rendered_artifact_id.
        Derived from stable forensic inputs to ensure replay parity.
        """
        id_seed = f"{artifact.normalized_url}|{artifact.crawl_task_id}|{artifact.attempt_number}|{self._version}"
        return hashlib.sha256(id_seed.encode('utf-8')).hexdigest()
