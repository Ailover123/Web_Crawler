from abc import ABC, abstractmethod
from typing import Optional
from rendering.models import RenderedArtifact

class RenderedArtifactStore(ABC):
    """
    Abstract storage interface for post-JS snapshots.
    Rendering is an enrichment layer; persistence is decoupled from raw artifacts.
    """
    @abstractmethod
    def save(self, artifact: RenderedArtifact) -> None:
        """Atomically persist a rendered snapshot."""
        pass

    @abstractmethod
    def get_by_parent(self, parent_artifact_id: str) -> Optional[RenderedArtifact]:
        """Retrieve the rendered enrichment for a specific raw crawl artifact."""
        pass
