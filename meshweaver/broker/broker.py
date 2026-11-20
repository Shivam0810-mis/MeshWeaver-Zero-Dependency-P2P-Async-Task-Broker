"""Core in-memory task broker: registration, validation, queue, and lookup."""

import json
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .interfaces import TaskQueueProtocol
from .models import Task, TaskNotFoundError, TaskStatus, TaskValidationError
from .queue import InMemoryTaskQueue


def _validate_json_value(value: Any, path: str) -> None:
    """Ensure ``value`` is strictly JSON-compatible (no non-string dict keys)."""
    if value is None or isinstance(value, (bool, int, str)):
        return
    if isinstance(value, float):
        try:
            json.dumps(value, allow_nan=False)
        except ValueError as exc:
            raise TaskValidationError(
                f"Non-finite float not allowed in JSON task payload at {path}."
            ) from exc
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TaskValidationError(
                    f"JSON object keys must be strings at {path}; got {type(key).__name__}."
                )
            _validate_json_value(item, f"{path}.{key}")
        return
    raise TaskValidationError(
        f"Unsupported non-JSON value of type {type(value).__name__} at {path}."
    )


def _canonical_payload_fingerprint(
    name: str,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    max_attempts: int,
) -> str:
    """Validate JSON compatibility and return a canonical payload fingerprint."""
    _validate_json_value(args, "args")
    _validate_json_value(kwargs, "kwargs")
    try:
        return json.dumps(
            {
                "name": name,
                "args": list(args),
                "kwargs": kwargs,
                "max_attempts": max_attempts,
            },
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise TaskValidationError(
            f"Task payload must be JSON-compatible: {exc}"
        ) from exc


class Broker:
    """Manages task registration, validation, pending queue, and status queries.

    Note on deduplication:
        ``submit_task_once`` / ``submission_key`` performs local in-memory
        deduplication for a single broker instance. It prevents duplicate
        enqueues from repeated client calls on this broker, but does NOT
        guarantee distributed exactly-once execution across restarts, multiple
        brokers, or lost network acknowledgements (at-least-once semantics).
    """

    def __init__(self, queue: Optional[TaskQueueProtocol] = None) -> None:
        self._tasks: Dict[str, Task] = {}
        self._queue: TaskQueueProtocol = (
            queue if queue is not None else InMemoryTaskQueue()
        )
        self._submission_keys: Dict[str, Tuple[str, str]] = {}

    def register_task(
        self,
        name: str,
        *,
        args: Iterable[Any] = (),
        kwargs: Optional[Dict[str, Any]] = None,
        max_attempts: int = 3,
        submission_key: Optional[str] = None,
    ) -> Task:
        """Validate, register, and enqueue a task with optional options."""
        if not isinstance(name, str) or not name.strip():
            raise TaskValidationError("Task name must be a non-empty string.")
        normalized_name = name.strip()

        if isinstance(args, (str, bytes, dict)):
            raise TaskValidationError(
                "Task args must be a positional sequence (tuple or list)."
            )
        try:
            normalized_args = tuple(args)
        except TypeError as exc:
            raise TaskValidationError(
                "Task args must be an iterable of positional arguments."
            ) from exc

        normalized_kwargs: Dict[str, Any] = (
            {} if kwargs is None else dict(kwargs)
        ) if isinstance(kwargs, (dict, type(None))) else self._raise_invalid_kwargs()

        if isinstance(max_attempts, bool) or not isinstance(
            max_attempts, int
        ) or max_attempts < 1:
            raise TaskValidationError("max_attempts must be an integer >= 1.")

        fingerprint = _canonical_payload_fingerprint(
            normalized_name,
            normalized_args,
            normalized_kwargs,
            max_attempts,
        )

        normalized_key: Optional[str] = None
        if submission_key is not None:
            if not isinstance(submission_key, str) or not submission_key.strip():
                raise TaskValidationError(
                    "Submission key must be a non-empty string."
                )
            normalized_key = submission_key.strip()

            existing_entry = self._submission_keys.get(normalized_key)
            if existing_entry is not None:
                existing_task_id, existing_fingerprint = existing_entry
                if existing_fingerprint != fingerprint:
                    raise TaskValidationError(
                        f"Submission key {normalized_key!r} was already used "
                        "with a different task payload."
                    )
                return self._tasks[existing_task_id]

        task = Task(
            name=normalized_name,
            args=normalized_args,
            kwargs=normalized_kwargs,
            max_attempts=max_attempts,
        )

        self._tasks[task.task_id] = task
        self._queue.enqueue(task.task_id)

        if normalized_key is not None:
            self._submission_keys[normalized_key] = (task.task_id, fingerprint)

        return task

    @staticmethod
    def _raise_invalid_kwargs() -> Dict[str, Any]:
        raise TaskValidationError("Task kwargs must be a dictionary.")

    def submit_task(
        self,
        name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Task:
        """Validate, register, and enqueue a new task."""
        return self.register_task(name, args=args, kwargs=kwargs)

    def submit_task_once(
        self,
        submission_key: str,
        name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Task:
        """Submit a task idempotently for a given local submission key.

        Re-submitting the same key with an identical payload returns the
        original ``Task`` without adding another queue entry. Re-submitting
        the same key with a conflicting payload raises ``TaskValidationError``.
        """
        if not isinstance(submission_key, str) or not submission_key.strip():
            raise TaskValidationError(
                "Submission key must be a non-empty string."
            )
        return self.register_task(
            name,
            args=args,
            kwargs=kwargs,
            submission_key=submission_key,
        )

    def get_task(self, task_id: str) -> Task:
        """Return a registered task by ID or raise ``TaskNotFoundError``."""
        if task_id not in self._tasks:
            raise TaskNotFoundError(f"Task not found: {task_id}")
        return self._tasks[task_id]

    def has_task(self, task_id: str) -> bool:
        """Return True if ``task_id`` is registered in the broker."""
        return task_id in self._tasks

    def get_task_status(self, task_id: str) -> TaskStatus:
        """Return the current ``TaskStatus`` for ``task_id``."""
        return self.get_task(task_id).status

    def get_task_summary(self, task_id: str) -> Dict[str, Any]:
        """Return a JSON-compatible dictionary summary of ``task_id``."""
        return self.get_task(task_id).to_dict()

    def list_tasks(
        self,
        status: Optional[TaskStatus] = None,
    ) -> List[Task]:
        """Return registered tasks, optionally filtered by lifecycle status."""
        if status is None:
            return list(self._tasks.values())
        if not isinstance(status, TaskStatus):
            raise TaskValidationError(f"Invalid status filter: {status!r}")
        return [
            task for task in self._tasks.values() if task.status == status
        ]

    def dequeue_task(self) -> Optional[Task]:
        """Dequeue and return the next pending task, or None if queue is empty."""
        task_id = self._queue.dequeue_nowait()
        if task_id is None:
            return None
        return self._tasks[task_id]

    async def wait_for_task(self) -> Task:
        """Await and return the next pending task from the broker queue."""
        task_id = await self._queue.dequeue()
        return self._tasks[task_id]

    def requeue_task(self, task_id: str, *, front: bool = False) -> None:
        """Re-enqueue a registered ``QUEUED`` task without losing work."""
        task = self.get_task(task_id)
        if task.status != TaskStatus.QUEUED:
            raise TaskValidationError(
                f"Only QUEUED tasks can be requeued; task {task_id} is "
                f"{task.status.value}."
            )
        if isinstance(self._queue, InMemoryTaskQueue) and self._queue.contains(
            task_id
        ):
            return
        if front:
            self._queue.requeue_front(task_id)
        else:
            self._queue.enqueue(task_id)

    def task_count(self) -> int:
        """Return total number of registered tasks."""
        return len(self._tasks)

    def pending_count(self) -> int:
        """Return number of tasks currently waiting in the pending queue."""
        return self._queue.size()
