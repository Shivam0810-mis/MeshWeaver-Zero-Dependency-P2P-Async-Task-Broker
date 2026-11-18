import asyncio
from typing import Any, Dict, Optional

from .models import Task


class Broker:
    def __init__(self):
        self._tasks: Dict[str, Task] = {}
        self._pending: asyncio.Queue = asyncio.Queue()
        self._submission_keys: Dict[str, str] = {}

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

    def task_count(self) -> int:
        return len(self._tasks)

    def pending_count(self) -> int:
        return self._pending.qsize()