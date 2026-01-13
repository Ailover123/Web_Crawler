from abc import ABC, abstractmethod
from typing import Optional, List
from frontier.models import FrontierTask, TaskState

class TaskStore(ABC):
    """
    Abstract interface for Frontier task storage with CAS (Compare-And-Swap) support.
    Ensures race-safe state transitions and deduplication within a specific crawl session.
    """

    @property
    @abstractmethod
    def session_id(self) -> str:
        """The session this store is bound to."""
        pass

    @abstractmethod
    def create_if_absent(self, task: FrontierTask) -> bool:
        """
        Atomically create task ONLY if (session_id, normalized_url) does not exist.
        Returns True if created, False if already exists.
        """
        pass

    @abstractmethod
    def transition(
        self,
        normalized_url: str,
        from_state: TaskState,
        to_task: FrontierTask
    ) -> bool:
        """
        Atomically replace task ONLY if current state == from_state 
        for the current session_id.
        Returns True if transition succeeded, False otherwise.
        """
        pass

    @abstractmethod
    def get(self, normalized_url: str) -> Optional[FrontierTask]:
        """Retrieve a task for the current session by its URL."""
        pass

    @abstractmethod
    def next_pending(self) -> Optional[FrontierTask]:
        """Retrieve the next PENDING task for the current session by priority."""
        pass

    @abstractmethod
    def get_expired_leases(self, crash_threshold_seconds: int) -> List[FrontierTask]:
        """Retrieve tasks in ASSIGNED state with stale heartbeats for the current session."""
        pass
