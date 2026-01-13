from abc import ABC, abstractmethod
from typing import Optional, List
from frontier.models import FrontierTask, TaskState

class TaskStore(ABC):
    """
    Abstract interface for Frontier task storage with CAS (Compare-And-Swap) support.
    Ensures race-safe state transitions and deduplication.
    """
    @abstractmethod
    def create_if_absent(self, task: FrontierTask) -> bool:
        """
        Atomically create task ONLY if normalized_url does not exist.
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
        Atomically replace task ONLY if current state == from_state.
        Returns True if transition succeeded, False otherwise.
        """
        pass

    @abstractmethod
    def get(self, normalized_url: str) -> Optional[FrontierTask]:
        """Retrieve a task by its primary key."""
        pass

    @abstractmethod
    def next_pending(self) -> Optional[FrontierTask]:
        """Retrieve the next PENDING task by priority."""
        pass

    @abstractmethod
    def get_expired_leases(self, crash_threshold_seconds: int) -> List[FrontierTask]:
        """Retrieve tasks in ASSIGNED state with stale heartbeats."""
        pass
