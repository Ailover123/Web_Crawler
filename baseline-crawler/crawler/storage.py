from abc import ABC, abstractmethod
from crawler.models import CrawlResponse

class ArtifactWriter(ABC):
    """
    Abstract interface for logging crawl events.
    Phase 1 Output: Metadata logging ONLY.
    """
    @abstractmethod
    def write(self, response: CrawlResponse, page_version_id: Optional[str] = None) -> None:
        """
        Atomically write crawl metadata to history.
        Raw body is explicitly ignored.
        Links to Normalized Content (PageVersion) if available.
        """
        pass
