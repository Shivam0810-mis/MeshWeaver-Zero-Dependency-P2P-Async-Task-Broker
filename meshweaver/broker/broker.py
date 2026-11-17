from typing import Any, Dict

from .models import Task

class Broker:
    def __init__(self):
        self._tasks: Dict[str, Task] = {}


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
        return task


    def get_task(self, task_id: str) -> Task:
        if task_id not in self._tasks:
            raise KeyError(f"Task not found: {task_id}")

        return self._tasks[task_id]

    def task_count(self) -> int:
        return len(self._tasks)
