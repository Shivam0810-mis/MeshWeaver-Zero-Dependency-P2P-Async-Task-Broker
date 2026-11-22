"""Broker task lifecycle states, exceptions, and task model."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, FrozenSet, Optional, Tuple
from uuid import uuid4
from ..core.errors import TaskError


class TaskValidationError(TaskError, ValueError):
    """Raised when task input, payload, or configuration is invalid."""


class TaskNotFoundError(TaskError, KeyError):
    """Raised when a requested task ID does not exist in the broker."""

    def __str__(self) -> str:
        return Exception.__str__(self)


class InvalidTaskTransitionError(TaskError, ValueError):
    """Raised when an unsupported task lifecycle state transition occurs."""


def normalize_deadline(deadline_at: datetime) -> datetime:
    """Validate that ``deadline_at`` is a timezone-aware datetime and convert to UTC."""
    if not isinstance(deadline_at, datetime):
        raise TaskValidationError("Task deadline must be a datetime.")
    if deadline_at.tzinfo is None or deadline_at.utcoffset() is None:
        raise TaskValidationError("Task deadline must be timezone-aware.")
    return deadline_at.astimezone(timezone.utc)


class TaskStatus(str, Enum):
    """Lifecycle states of a task managed by the broker."""

    QUEUED = "QUEUED"
    ASSIGNING = "ASSIGNING"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"

    @property
    def is_terminal(self) -> bool:
        """Return True when the task has reached a final state."""
        return self in TERMINAL_STATUSES


TERMINAL_STATUSES: FrozenSet[TaskStatus] = frozenset(
    {
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.EXPIRED,
    }
)

ALLOWED_TRANSITIONS: Dict[TaskStatus, FrozenSet[TaskStatus]] = {
    TaskStatus.QUEUED: frozenset(
        {
            TaskStatus.ASSIGNING,
            TaskStatus.EXPIRED,
            TaskStatus.FAILED,
        }
    ),
    TaskStatus.ASSIGNING: frozenset(
        {
            TaskStatus.ASSIGNED,
            TaskStatus.QUEUED,
            TaskStatus.RETRY_WAIT,
            TaskStatus.FAILED,
            TaskStatus.EXPIRED,
        }
    ),
    TaskStatus.ASSIGNED: frozenset(
        {
            TaskStatus.RUNNING,
            TaskStatus.COMPLETED,
            TaskStatus.RETRY_WAIT,
            TaskStatus.FAILED,
            TaskStatus.EXPIRED,
        }
    ),
    TaskStatus.RUNNING: frozenset(
        {
            TaskStatus.COMPLETED,
            TaskStatus.RETRY_WAIT,
            TaskStatus.FAILED,
            TaskStatus.EXPIRED,
        }
    ),
    TaskStatus.RETRY_WAIT: frozenset(
        {
            TaskStatus.QUEUED,
            TaskStatus.FAILED,
            TaskStatus.EXPIRED,
        }
    ),
    TaskStatus.COMPLETED: frozenset(),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.EXPIRED: frozenset(),
}


@dataclass
class Task:
    """In-memory broker representation of a submitted task."""

    name: str
    args: Tuple[Any, ...] = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    max_attempts: int = 3
    deadline_at: Optional[datetime] = None

    task_id: str = field(
        default_factory=lambda: str(uuid4()),
        init=False,
    )
    status: TaskStatus = field(
        default=TaskStatus.QUEUED,
        init=False,
    )
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
        init=False,
    )
    updated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
        init=False,
    )
    attempt_count: int = field(default=0, init=False)
    last_error: Optional[str] = field(default=None, init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise TaskValidationError(
                "Task name must be a non-empty string."
            )
        self.name = self.name.strip()

        if isinstance(self.max_attempts, bool) or not isinstance(
            self.max_attempts, int
        ) or self.max_attempts < 1:
            raise TaskValidationError(
                "max_attempts must be an integer >= 1."
            )

        if not isinstance(self.args, tuple):
            if isinstance(self.args, list):
                self.args = tuple(self.args)
            else:
                raise TaskValidationError("Task args must be a tuple or list.")

        if not isinstance(self.kwargs, dict):
            raise TaskValidationError("Task kwargs must be a dictionary.")

        if self.deadline_at is not None:
            self.deadline_at = normalize_deadline(self.deadline_at)

        self.updated_at = self.created_at

    def is_expired(self, now: Optional[datetime] = None) -> bool:
        """Return True if the task is already EXPIRED or past ``deadline_at``."""
        if self.status == TaskStatus.EXPIRED:
            return True
        if self.deadline_at is None:
            return False
        current_time = (
            datetime.now(timezone.utc)
            if now is None
            else normalize_deadline(now)
        )
        return current_time >= self.deadline_at

    @property
    def is_terminal(self) -> bool:
        """Return True if the task is COMPLETED, FAILED, or EXPIRED."""
        return self.status.is_terminal

    @property
    def attempts_remaining(self) -> int:
        """Return remaining attempts before reaching ``max_attempts``."""
        return max(0, self.max_attempts - self.attempt_count)

    @property
    def can_retry(self) -> bool:
        """Return True if task is non-terminal and below attempt limit."""
        return not self.is_terminal and self.attempt_count < self.max_attempts

    def can_transition_to(self, new_status: TaskStatus) -> bool:
        """Return True if transitioning from current status to new_status is valid."""
        if not isinstance(new_status, TaskStatus):
            return False
        return new_status in ALLOWED_TRANSITIONS[self.status]

    def transition_to(
        self,
        new_status: TaskStatus,
        *,
        error: Optional[str] = None,
    ) -> None:
        """Move task to ``new_status`` if allowed by the lifecycle state machine."""
        if not isinstance(new_status, TaskStatus):
            raise InvalidTaskTransitionError(
                f"Invalid task status: {new_status!r}"
            )

        if not self.can_transition_to(new_status):
            raise InvalidTaskTransitionError(
                f"Cannot transition task {self.task_id} from "
                f"{self.status.value} to {new_status.value}."
            )

        self.status = new_status
        self.updated_at = datetime.now(timezone.utc)
        if error is not None:
            self.last_error = error

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-compatible dictionary snapshot of the task."""
        return {
            "task_id": self.task_id,
            "name": self.name,
            "args": list(self.args),
            "kwargs": dict(self.kwargs),
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "attempt_count": self.attempt_count,
            "max_attempts": self.max_attempts,
            "last_error": self.last_error,
            "deadline_at": (
                self.deadline_at.isoformat()
                if self.deadline_at is not None
                else None
            ),
        }
