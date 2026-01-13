from datetime import datetime
from typing import Optional, List
import dataclasses

from frontier.models import FrontierTask, TaskState
from frontier.storage import TaskStore

class Frontier:
    """
    Implementation of Phase 2: Frontier & Orchestration.
    Hardened with CAS (Compare-And-Swap) for production concurrency safety.
    """

    def __init__(self, store: TaskStore, max_retries: int = 3, crash_threshold_seconds: int = 300):
        self._store = store
        self._max_retries = max_retries
        self._crash_threshold_seconds = crash_threshold_seconds

    def discover(self, urls: List[str], depth: int) -> None:
        """
        Deduplication Rule: Enforced atomically via create_if_absent.
        """
        for url in urls:
            task = FrontierTask(
                session_id=self._store.session_id,
                normalized_url=url,
                state=TaskState.PENDING, # Transition DISCOVERED -> PENDING folded into creation
                depth=depth
            )
            # INVARIANT: Atomic create-if-absent prevents duplicate task races.
            self._store.create_if_absent(task)

    def assign_next(self) -> Optional[FrontierTask]:
        """
        PENDING -> ASSIGNED transition.
        Uses CAS to ensure one-and-only-one worker assignment.
        """
        task = self._store.next_pending()
        if not task:
            return None

        assigned_task = dataclasses.replace(
            task,
            state=TaskState.ASSIGNED,
            attempt_count=task.attempt_count + 1,
            last_heartbeat=datetime.utcnow()
        )
        
        # INVARIANT: Transition only if still PENDING. Prevents double-assignment races.
        if self._store.transition(task.normalized_url, TaskState.PENDING, assigned_task):
            return assigned_task
        
        return None # Someone else won the race

    def report_success(self, normalized_url: str) -> None:
        """
        ASSIGNED -> COMPLETED transition.
        """
        task = self._store.get(normalized_url)
        if task and task.state == TaskState.ASSIGNED:
            completed_task = dataclasses.replace(task, state=TaskState.COMPLETED)
            # INVARIANT: Transition only if still ASSIGNED. Prevents stale success reports.
            self._store.transition(normalized_url, TaskState.ASSIGNED, completed_task)

    def report_failure(self, normalized_url: str) -> None:
        """
        ASSIGNED -> PENDING or ASSIGNED -> FAILED transition.
        Total state transition logic with CAS safety.
        """
        task = self._store.get(normalized_url)
        if not task or task.state != TaskState.ASSIGNED:
            return

        if task.attempt_count < self._max_retries:
            new_task = dataclasses.replace(task, state=TaskState.PENDING)
            # INVARIANT: Atomic transition back to PENDING.
            self._store.transition(normalized_url, TaskState.ASSIGNED, new_task)
        else:
            new_task = dataclasses.replace(task, state=TaskState.FAILED)
            # INVARIANT: Atomic transition to FAILED.
            self._store.transition(normalized_url, TaskState.ASSIGNED, new_task)

    def recover_crashes(self) -> None:
        """
        Lease / Heartbeat handling.
        Race-safe recovery via conditional transition.
        """
        expired_tasks = self._store.get_expired_leases(self._crash_threshold_seconds)
        for task in expired_tasks:
            recovered_task = dataclasses.replace(task, state=TaskState.PENDING)
            # INVARIANT: Only reset if still ASSIGNED. Prevents recovery vs late-heartbeat races.
            self._store.transition(task.normalized_url, TaskState.ASSIGNED, recovered_task)

    def update_heartbeat(self, normalized_url: str) -> None:
        """
        Worker Signal: Updates last_heartbeat only if task is still ASSIGNED.
        Prevents heartbeat resurrection after recovery.
        """
        task = self._store.get(normalized_url)
        if task and task.state == TaskState.ASSIGNED:
            active_task = dataclasses.replace(task, last_heartbeat=datetime.utcnow())
            # INVARIANT: CAS ensures heartbeats never resurrect a recovered/completed task.
            self._store.transition(normalized_url, TaskState.ASSIGNED, active_task)
