from abc import ABC, abstractmethod
from typing import Optional
from baseline.models import BaselineProfile

class BaselineStore(ABC):
    """
    Abstract interface for managing versioned site baselines and immutable profiles.
    """

    @abstractmethod
    def get_active_baseline_id(self, site_id: int) -> Optional[str]:
        """Retrieve the ID of the currently ACTIVE baseline for a site."""
        pass

    @abstractmethod
    def save_profile(self, profile: BaselineProfile) -> None:
        """Atomically persist a profile. Must fail if profile_id exists."""
        pass

    @abstractmethod
    def get_profile(self, baseline_id: str, normalized_url: str) -> Optional[BaselineProfile]:
        """Retrieve a specific profile within a site baseline."""
        pass

    @abstractmethod
    def promote_baseline(
        self,
        siteid: int,
        baseline_id: str,
        actor_id: Optional[str] = None
    ) -> None:
        """
        Atomically promote a baseline to ACTIVE for a site.
        Raises an exception if the baseline does not belong to the site
        or if promotion invariants are violated.
        """
        pass
