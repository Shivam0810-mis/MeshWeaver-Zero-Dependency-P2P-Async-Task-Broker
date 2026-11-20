"""Broker and scheduling interfaces (integration boundaries).

These ``typing.Protocol`` contracts define the Member 2 (Broker & Scheduling)
API and clean boundaries to Member 1 (Network transport) and Member 4
(shared ``Message`` protocol models) without introducing third-party
dependencies.
"""

from typing import Any, Iterable, Optional, Protocol, runtime_checkable

from ..models.message import Message

from .models import Task, TaskStatus
from .peers import PeerInfo


@runtime_checkable
class TaskQueueProtocol(Protocol):
    """In-memory pending-task queue contract owned by the broker."""

    def enqueue(self, task_id: str) -> None:
        """Append a pending task ID to the back of the queue."""
        ...

    def requeue_front(self, task_id: str) -> None:
        """Put a task ID back at the front (e.g., after dispatch rollback)."""
        ...

    def dequeue_nowait(self) -> Optional[str]:
        """Pop the next task ID without blocking, or return None if empty."""
        ...

    async def dequeue(self) -> str:
        """Await and return the next pending task ID."""
        ...

    def size(self) -> int:
        """Return the number of currently queued task IDs."""
        ...

    def is_empty(self) -> bool:
        """Return True when no task IDs are waiting in the queue."""
        ...


@runtime_checkable
class BrokerProtocol(Protocol):
    """Core task registration, queue, and lookup contract."""

    def submit_task(
        self,
        name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Task:
        """Validate, register, and enqueue a new task."""
        ...

    def get_task(self, task_id: str) -> Task:
        """Return a registered task by ID or raise TaskNotFoundError/KeyError."""
        ...

    def get_task_status(self, task_id: str) -> TaskStatus:
        """Return the current lifecycle status for a task ID."""
        ...

    def dequeue_task(self) -> Optional[Task]:
        """Dequeue and return the next pending task, or None if empty."""
        ...

    def task_count(self) -> int:
        """Return total number of registered tasks."""
        ...

    def pending_count(self) -> int:
        """Return number of tasks currently queued for dispatch."""
        ...


@runtime_checkable
class PeerSelectorProtocol(Protocol):
    """Scheduling policy interface for selecting an eligible worker peer."""

    def select_peer(
        self,
        peers: Iterable[PeerInfo],
    ) -> Optional[PeerInfo]:
        """Select the best eligible peer, or return None if none qualify."""
        ...


@runtime_checkable
class TaskTransportProtocol(Protocol):
    """Integration boundary for sending shared protocol messages to peers.

    Member 1 owns the real asyncio TCP/NDJSON transport. Isolated broker tests
    supply a fake implementation of this protocol.
    """

    async def send_message(self, peer_id: str, message: Message) -> None:
        """Send a shared protocol Message to ``peer_id``."""
        ...
