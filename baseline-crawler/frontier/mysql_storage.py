import pymysql
import dataclasses
from datetime import datetime
from typing import Optional, List
from frontier.models import FrontierTask, TaskState
from frontier.storage import TaskStore

class MySQLTaskStore(TaskStore):
    """
    MySQL implementation of TaskStore.
    Uses InnoDB for ACID compliance and CAS safety.
    """

    def __init__(self, connection_pool, session_id: str):
        self._pool = connection_pool
        self._session_id = session_id

    @property
    def session_id(self) -> str:
        return self._session_id

    def create_if_absent(self, task: FrontierTask) -> bool:
        """Atomically create task ONLY if (session_id, normalized_url) does not exist."""
        sql = """
            INSERT IGNORE INTO task_store (
                session_id, normalized_url, state, attempt_count, 
                last_heartbeat, priority, depth
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        with self._pool.cursor() as cursor:
            affected = cursor.execute(sql, (
                self._session_id, task.normalized_url, task.state.value,
                task.attempt_count, task.last_heartbeat, task.priority, task.depth
            ))
            self._pool.commit()
            return affected > 0

    def next_pending(self) -> Optional[FrontierTask]:
        """
        Find and atomically transition the next PENDING task to ASSIGNED.
        Ensures FOR UPDATE SKIP LOCKED works within a transaction boundary.
        """
        select_sql = """
            SELECT session_id, normalized_url, state, attempt_count, last_heartbeat, priority, depth
            FROM task_store
            WHERE session_id = %s AND state = 'PENDING'
            ORDER BY priority DESC, last_heartbeat ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """
        update_sql = """
            UPDATE task_store 
            SET state = 'ASSIGNED', attempt_count = attempt_count + 1, last_heartbeat = %s
            WHERE session_id = %s AND normalized_url = %s AND state = 'PENDING'
        """

        with self._pool.cursor() as cursor:
            try:
                # 1. Start Explicit Transaction
                self._pool.begin()
                
                # 2. Select with Lock
                cursor.execute(select_sql, (self._session_id,))
                row = cursor.fetchone()
                if not row:
                    self._pool.rollback()
                    return None

                # 3. Update within valid lock
                now = datetime.utcnow()
                cursor.execute(update_sql, (now, self._session_id, row[1]))
                
                # 4. Commit and return the updated task
                self._pool.commit()
                return FrontierTask(
                    session_id=row[0],
                    normalized_url=row[1],
                    state=TaskState.ASSIGNED,
                    attempt_count=row[3] + 1,
                    last_heartbeat=now,
                    priority=row[5],
                    depth=row[6]
                )
            except Exception:
                self._pool.rollback()
                raise

    def transition(
        self,
        normalized_url: str,
        from_state: TaskState,
        to_task: FrontierTask
    ) -> bool:
        """
        CAS transition: Update only if current state matches expected.
        Acts as the primary safety gate for state mutations.
        """
        sql = """
            UPDATE task_store 
            SET state = %s, attempt_count = %s, last_heartbeat = %s, priority = %s, depth = %s
            WHERE session_id = %s AND normalized_url = %s AND state = %s
        """
        with self._pool.cursor() as cursor:
            affected = cursor.execute(sql, (
                to_task.state.value, to_task.attempt_count, to_task.last_heartbeat,
                to_task.priority, to_task.depth,
                self._session_id, normalized_url, from_state.value
            ))
            self._pool.commit()
            return affected > 0

    def get(self, normalized_url: str) -> Optional[FrontierTask]:
        sql = """
            SELECT session_id, normalized_url, state, attempt_count, last_heartbeat, priority, depth
            FROM task_store
            WHERE session_id = %s AND normalized_url = %s
        """
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (self._session_id, normalized_url))
            row = cursor.fetchone()
            if row:
                return self._row_to_task(row)
        return None

    def get_expired_leases(self, crash_threshold_seconds: int) -> List[FrontierTask]:
        sql = """
            SELECT session_id, normalized_url, state, attempt_count, last_heartbeat, priority, depth
            FROM task_store
            WHERE session_id = %s AND state = 'ASSIGNED'
            AND last_heartbeat < (NOW() - INTERVAL %s SECOND)
        """
        tasks = []
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (self._session_id, crash_threshold_seconds))
            for row in cursor.fetchall():
                tasks.append(self._row_to_task(row))
        return tasks

    def _row_to_task(self, row) -> FrontierTask:
        return FrontierTask(
            session_id=row[0],
            normalized_url=row[1],
            state=TaskState(row[2]),
            attempt_count=row[3],
            last_heartbeat=row[4],
            priority=row[5],
            depth=row[6]
        )
