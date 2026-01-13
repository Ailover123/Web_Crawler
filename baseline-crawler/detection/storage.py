from abc import ABC, abstractmethod
from typing import Optional, List
from detection.models import DetectionVerdict

class DetectionVerdictStore(ABC):
    """
    Abstract interface for storing immutable defacement analysis verdicts.
    """

    @abstractmethod
    def save(self, verdict: DetectionVerdict) -> None:
        """Atomically persist a verdict."""
        pass

    @abstractmethod
    def get_latest(self, normalized_url: str) -> Optional[DetectionVerdict]:
        """Retrieve the most recent analysis result for a URL."""
        pass
