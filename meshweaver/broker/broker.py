import asyncio
import json
from typing import Any, Dict, Optional
from .scheduler import scheduler
from .models import Task, TaskStatus
from meshweaver.core.errors import StorageError


class Broker:
    def __init__(self, database=None, task_scheduler=None):
        self._tasks: Dict[str, Task] = {}
        self._pending: asyncio.Queue = asyncio.Queue()
        self._submission_keys: Dict[str, str] = {}
        self._database = database
        self._scheduler = (
            task_scheduler if task_scheduler is not None else scheduler()
        )

    def submit_task(
        self,
        name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Task:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Task name must be a non-empty string.")

        task = Task(
            name=name.strip(),
            args=tuple(args),
            kwargs=dict(kwargs),
        )

        # Persist first; only enqueue after successful storage.
        if self._database is not None:
            payload = json.dumps({
                "args": task.args,
                "kwargs": task.kwargs,
            })

            try:
                self._database.add_task(
                    task_id=task.task_id,
                    task_type=task.name,
                    payload=payload,
                    status=task.status.value,
                )
            except Exception as exc:
                raise StorageError(
                    f"Could not persist task {task.task_id}: {exc}"
                ) from exc

        self._tasks[task.task_id] = task
        self._pending.put_nowait(task.task_id)

        return task

    def submit_task_once(
        self,
        submission_key: str,
        name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Task:
        if (
            not isinstance(submission_key, str)
            or not submission_key.strip()
        ):
            raise ValueError(
                "Submission key must be a non-empty string."
            )

        if submission_key in self._submission_keys:
            task_id = self._submission_keys[submission_key]
            return self._tasks[task_id]

        task = self.submit_task(name, *args, **kwargs)
        self._submission_keys[submission_key] = task.task_id

        return task

    def get_task(self, task_id: str) -> Task:
        if task_id not in self._tasks:
            raise KeyError(f"Task not found: {task_id}")

        return self._tasks[task_id]

    def dequeue_task(self) -> Optional[Task]:
        try:
            task_id = self._pending.get_nowait()
        except asyncio.QueueEmpty:
            return None

        self._pending.task_done()

        return self._tasks[task_id]

    def assign_next_task(self, peers):
        """Assign the next pending task to an eligible peer."""
        task = self.dequeue_task()

        if task is None:
            return None

        try:
            selected_peer = self._scheduler.select_peer(peers)
        except Exception:
            self._pending.put_nowait(task.task_id)
            raise

        if selected_peer is None:
            self._pending.put_nowait(task.task_id)
            return None

        previous_status = task.status
        task.status = TaskStatus.ASSIGNED

        if self._database is not None:
            try:
                updated = self._database.update_task(
                    task_id=task.task_id,
                    status=task.status.value,
                    assigned_peer_id=selected_peer.peer_id,
                )

                if not updated:
                    raise ValueError("Task record not found in database")

            except Exception as exc:
                task.status = previous_status
                self._pending.put_nowait(task.task_id)
                raise StorageError(
                    f"Could not assign task {task.task_id}: {exc}"
                ) from exc

        return task, selected_peer

    def task_count(self) -> int:
        return len(self._tasks)

    def pending_count(self) -> int:
        return self._pending.qsize()
