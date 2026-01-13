from abc import ABC, abstractmethod
from crawler.models import CrawlArtifact

class ArtifactWriter(ABC):
    """
    Abstract interface for writing CrawlArtifacts.
    Follows Phase 3 architecture: Crawl Phase MUST only write to the Artifact Store.
    """
    @abstractmethod
    def write(self, artifact: CrawlArtifact) -> None:
        """
        Atomically write exactly one CrawlArtifact to the persistent store.
        """
        pass
