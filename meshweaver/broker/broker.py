
"""Core task broker with scheduling, storage, validation, and lookup."""

import json
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..core.errors import StorageError
from .interfaces import TaskQueueProtocol
from .models import Task, TaskNotFoundError, TaskStatus, TaskValidationError
from .queue import InMemoryTaskQueue
from .scheduler import scheduler


def _validate_json_value(value: Any, path: str) -> None:
    """Ensure value is strictly JSON-compatible."""
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
                    f"JSON object keys must be strings at {path}; "
                    f"got {type(key).__name__}."
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
    """Validate JSON compatibility and return a canonical fingerprint."""
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
    """Manages task registration, validation, pending queue, optional storage, and lookup.

    Note on deduplication:
        ``submit_task_once`` / ``submission_key`` performs local in-memory
        deduplication for a single broker instance. It prevents duplicate
        enqueues from repeated client calls on this broker, but does NOT
        guarantee distributed exactly-once execution across restarts, multiple
        brokers, or lost network acknowledgements (at-least-once semantics).
    """

    def __init__(
        self,
        database: Any = None,
        queue: Optional[TaskQueueProtocol] = None,
        task_scheduler: Any = None,
    ) -> None:
        self._tasks: Dict[str, Task] = {}
        self._queue: TaskQueueProtocol = (
            queue if queue is not None else InMemoryTaskQueue()
        )
        self._submission_keys: Dict[str, Tuple[str, str]] = {}
        self._database = database
        self._scheduler = (
            task_scheduler if task_scheduler is not None else scheduler()
        )

    def register_task(
        self,
        name: str,
        *,
        args: Iterable[Any] = (),
        kwargs: Optional[Dict[str, Any]] = None,
        max_attempts: int = 3,
        submission_key: Optional[str] = None,
    ) -> Task:
        """Validate, persist, register, and enqueue a task."""

        if not isinstance(name, str) or not name.strip():
            raise TaskValidationError(
                "Task name must be a non-empty string."
            )

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

        if kwargs is None:
            normalized_kwargs: Dict[str, Any] = {}
        elif isinstance(kwargs, dict):
            normalized_kwargs = dict(kwargs)
        else:
            raise TaskValidationError(
                "Task kwargs must be a dictionary."
            )

        if (
            isinstance(max_attempts, bool)
            or not isinstance(max_attempts, int)
            or max_attempts < 1
        ):
            raise TaskValidationError(
                "max_attempts must be an integer >= 1."
            )

        fingerprint = _canonical_payload_fingerprint(
            normalized_name,
            normalized_args,
            normalized_kwargs,
            max_attempts,
        )

        normalized_key: Optional[str] = None

        if submission_key is not None:
            if (
                not isinstance(submission_key, str)
                or not submission_key.strip()
            ):
                raise TaskValidationError(
                    "Submission key must be a non-empty string."
                )

            normalized_key = submission_key.strip()

            existing_entry = self._submission_keys.get(normalized_key)

            if existing_entry is not None:
                existing_task_id, existing_fingerprint = existing_entry

                if existing_fingerprint != fingerprint:
                    raise TaskValidationError(
                        f"Submission key {normalized_key!r} was already "
                        "used with a different task payload."
                    )

                return self._tasks[existing_task_id]

        task = Task(
            name=normalized_name,
            args=normalized_args,
            kwargs=normalized_kwargs,
            max_attempts=max_attempts,
        )

        # Persist before registering and enqueueing.
        if self._database is not None:
            payload = json.dumps(
                {
                    "args": list(task.args),
                    "kwargs": task.kwargs,
                },
                allow_nan=False,
            )

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
        self._queue.enqueue(task.task_id)

        if normalized_key is not None:
            self._submission_keys[normalized_key] = (
                task.task_id,
                fingerprint,
            )

        return task

    def submit_task(
        self,
        name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Task:
        """Submit a task."""
        return self.register_task(
            name,
            args=args,
            kwargs=kwargs,
        )

    def submit_task_once(
        self,
        submission_key: str,
        name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Task:
        """Submit a task idempotently using a local submission key."""

        if (
            not isinstance(submission_key, str)
            or not submission_key.strip()
        ):
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
        """Return a task by ID."""
        if task_id not in self._tasks:
            raise TaskNotFoundError(
                f"Task not found: {task_id}"
            )

        return self._tasks[task_id]

    def has_task(self, task_id: str) -> bool:
        """Check whether a task exists."""
        return task_id in self._tasks

    def get_task_status(self, task_id: str) -> TaskStatus:
        """Return task status."""
        return self.get_task(task_id).status

    def get_task_summary(self, task_id: str) -> Dict[str, Any]:
        """Return task summary."""
        return self.get_task(task_id).to_dict()

    def list_tasks(
        self,
        status: Optional[TaskStatus] = None,
    ) -> List[Task]:
        """Return all tasks, optionally filtered by status."""

        if status is None:
            return list(self._tasks.values())

        if not isinstance(status, TaskStatus):
            raise TaskValidationError(
                f"Invalid status filter: {status!r}"
            )

        return [
            task
            for task in self._tasks.values()
            if task.status == status
        ]

    def dequeue_task(self) -> Optional[Task]:
        """Dequeue the next pending task."""

        task_id = self._queue.dequeue_nowait()

        if task_id is None:
            return None

        return self._tasks[task_id]

    async def wait_for_task(self) -> Task:
        """Wait asynchronously for the next pending task."""

        task_id = await self._queue.dequeue()
        return self._tasks[task_id]

    def requeue_task(
        self,
        task_id: str,
        *,
        front: bool = False,
    ) -> None:
        """Re-enqueue a registered QUEUED task."""

        task = self.get_task(task_id)

        if task.status != TaskStatus.QUEUED:
            raise TaskValidationError(
                f"Only QUEUED tasks can be requeued; "
                f"task {task_id} is {task.status.value}."
            )

        if (
            isinstance(self._queue, InMemoryTaskQueue)
            and self._queue.contains(task_id)
        ):
            return

        if front:
            self._queue.requeue_front(task_id)
        else:
            self._queue.enqueue(task_id)

    def assign_next_task(self, peers):
        """Assign the next pending task to an eligible peer."""

        task = self.dequeue_task()

        if task is None:
            return None

        try:
            selected_peer = self._scheduler.select_peer(peers)
        except Exception:
            self.requeue_task(task.task_id, front=True)
            raise

        if selected_peer is None:
            self.requeue_task(task.task_id, front=True)
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
                    raise ValueError(
                        "Task record not found in database."
                    )

            except Exception as exc:
                task.status = previous_status
                self.requeue_task(task.task_id, front=True)

                raise StorageError(
                    f"Could not assign task {task.task_id}: {exc}"
                ) from exc

        return task, selected_peer

    def task_count(self) -> int:
        """Return total registered task count."""
        return len(self._tasks)

    def pending_count(self) -> int:
        """Return pending queue size."""
        return self._queue.size()
