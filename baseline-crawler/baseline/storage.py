from abc import ABC, abstractmethod
from typing import Optional
from baseline.models import BaselineProfile

class BaselineStore(ABC):
    """
    Abstract interface for storing BaselineProfiles.
    Selection Rule: At most one ACTIVE profile per (url, version).
    """

    @abstractmethod
    def save(self, profile: BaselineProfile) -> None:
        """
        Atomically persist an immutable BaselineProfile.
        Must fail if baseline_id already exists.
        """
        pass

    @abstractmethod
    def get_latest(self, normalized_url: str, extraction_version: str) -> Optional[BaselineProfile]:
        """
        Retrieve the most recent profile matching the url and version.
        """
        pass
