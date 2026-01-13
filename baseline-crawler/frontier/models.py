from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

class TaskState(Enum):
    DISCOVERED = "DISCOVERED"
    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

@dataclass(frozen=True)
class FrontierTask:
    """
    Data model for a Frontier task.
    Invariants: normalized_url is the Primary Key.
    """
    normalized_url: str
    state: TaskState
    attempt_count: int = 0
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)
    priority: int = 0
    depth: int = 0
