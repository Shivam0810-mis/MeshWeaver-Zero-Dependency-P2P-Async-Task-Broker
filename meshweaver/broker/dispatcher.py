"""Assignment creation, async dispatch boundary, ACK handling, and retry policy.

Design notes & integration boundaries:
    - ``TaskTransportProtocol`` is the integration boundary to Member 1's
      asyncio TCP/NDJSON transport. ``dispatch_next`` sends Member 4's shared
      ``Message(MessageType.TASK_SUBMIT, ...)`` through this boundary and rolls
      back the local assignment/requeues the task if sending fails.
    - ``handle_message`` is the protocol adapter boundary for ``TASK_ACK``,
      ``TASK_REJECT``, ``TASK_STARTED``, ``TASK_RESULT``, and ``TASK_FAILED``.
      ``TASK_ACK`` confirms worker acceptance (``ASSIGNING -> ASSIGNED``), NOT
      execution completion.
    - ``process_ack_timeouts`` enforces acceptance-ACK timeouts using an
      injectable monotonic clock (``ack_clock``). ACK timeout handling is
      distinct from worker execution timeouts (owned by Member 3): once an
      assignment is acknowledged (``ASSIGNED`` / ``RUNNING``), its ACK deadline
      is cleared.
    - Local capacity reservations only coordinate assignments within this
      broker instance. Another broker may target the same worker concurrently,
      so worker acceptance (``TASK_ACK`` / ``TASK_REJECT``) is authoritative.
    - Adding ``peer.running_tasks + self._reservations[peer.peer_id]`` can
      double-count once peer heartbeat snapshots already include accepted work;
      callers should pass external/unreserved counts or refresh snapshots
      consistently.
    - Network ACK loss can cause repeat execution (at-least-once semantics,
      not exactly-once).
"""

import math
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from time import monotonic
from typing import Any, Callable, Dict, Iterable, Optional
from uuid import uuid4

from ..models.enums import MessageType
from ..models.message import Message
from .broker import Broker
from .interfaces import (
    PeerSelectorProtocol,
    TaskQueueProtocol,
    TaskTransportProtocol,
)
from .models import Task, TaskStatus, TaskValidationError
from .peers import PeerInfo
from .scheduler import LeastLoadedScheduler


@dataclass(frozen=True)
class Assignment:
    """Immutable record pairing a task attempt with a target worker peer."""

    assignment_id: str
    task_id: str
    peer_id: str
    attempt_number: int = 1
    assigned_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-compatible dictionary representation."""
        return {
            "assignment_id": self.assignment_id,
            "task_id": self.task_id,
            "peer_id": self.peer_id,
            "attempt_number": self.attempt_number,
            "assigned_at": self.assigned_at.isoformat(),
        }

    def to_task_submit_message(
        self,
        task: Task,
        *,
        sender_id: str,
    ) -> Message:
        """Build a shared protocol ``TASK_SUBMIT`` message for dispatch."""
        return Message(
            message_type=MessageType.TASK_SUBMIT,
            sender_id=sender_id,
            payload={
                "assignment_id": self.assignment_id,
                "task_id": task.task_id,
                "name": task.name,
                "args": list(task.args),
                "kwargs": dict(task.kwargs),
                "attempt_number": self.attempt_number,
                "max_attempts": task.max_attempts,
                "assigned_at": self.assigned_at.isoformat(),
            },
        )


class DispatchBroker(Broker):
    """Extends ``Broker`` with scheduling, reservations, dispatch, ACKs, and retries."""

    def __init__(
        self,
        *,
        node_id: str = "broker-local",
        database: Any = None,
        scheduler: Optional[PeerSelectorProtocol] = None,
        queue: Optional[TaskQueueProtocol] = None,
        clock: Optional[Callable[[], datetime]] = None,
        ack_timeout: float = 5.0,
        ack_clock: Callable[[], float] = monotonic,
    ) -> None:
        if (
            isinstance(ack_timeout, bool)
            or not isinstance(ack_timeout, (int, float))
            or not math.isfinite(ack_timeout)
            or ack_timeout <= 0
        ):
            raise TaskValidationError(
                "ACK timeout must be a finite positive number."
            )

        if not callable(ack_clock):
            raise TypeError("ack_clock must be callable.")

        super().__init__(database=database, queue=queue, clock=clock)
        if not isinstance(node_id, str) or not node_id.strip():
            raise TaskValidationError("node_id must be a non-empty string.")
        self._node_id = node_id.strip()
        self._scheduler: PeerSelectorProtocol = (
            scheduler if scheduler is not None else LeastLoadedScheduler()
        )
        self._reservations: Counter[str] = Counter()
        self._active: Dict[str, Assignment] = {}
        self._active_by_task: Dict[str, str] = {}
        self._ack_timeout: float = float(ack_timeout)
        self._ack_clock: Callable[[], float] = ack_clock
        self._ack_deadlines: Dict[str, float] = {}

    @property
    def node_id(self) -> str:
        """Return the broker node identifier used on outbound protocol messages."""
        return self._node_id

    @property
    def ack_timeout(self) -> float:
        """Return the configured acceptance-ACK timeout in seconds."""
        return self._ack_timeout

    def _release_reservation(self, peer_id: str) -> None:
        if self._reservations[peer_id] > 1:
            self._reservations[peer_id] -= 1
        else:
            self._reservations.pop(peer_id, None)

    def _release_assignment(
        self,
        assignment_id: str,
    ) -> Optional[Assignment]:
        """Remove an active assignment, clear its ACK deadline, and free capacity."""
        assignment = self._active.pop(assignment_id, None)
        self._ack_deadlines.pop(assignment_id, None)

        if assignment is None:
            return None

        if self._active_by_task.get(assignment.task_id) == assignment_id:
            self._active_by_task.pop(assignment.task_id, None)

        self._release_reservation(assignment.peer_id)
        return assignment

    def _expire_task_if_due(
        self,
        task: Task,
        *,
        now: Optional[datetime] = None,
    ) -> bool:
        """Expire overdue task and release any active assignment/reservation."""
        expired = super()._expire_task_if_due(task, now=now)
        if expired:
            assignment_id = self._active_by_task.get(task.task_id)
            if assignment_id is not None:
                self._release_assignment(assignment_id)
        return expired

    def _rollback_unconfirmed_assignment(
        self,
        assignment: Assignment,
        *,
        error: Optional[str] = None,
    ) -> None:
        """Undo a local assignment when dispatch fails before peer acceptance."""
        self._release_assignment(assignment.assignment_id)

        task = self.get_task(assignment.task_id)
        if task.attempt_count > 0:
            task.attempt_count -= 1
        if task.status == TaskStatus.ASSIGNING:
            task.transition_to(TaskStatus.QUEUED, error=error)
            self.requeue_task(task.task_id, front=True)

    def assign_next(
        self,
        peers: Iterable[PeerInfo],
    ) -> Optional[Assignment]:
        """Select an eligible peer and create a local assignment for the next task.

        Returns ``None`` without consuming the queue when no peer is eligible,
        or without reserving capacity when the queue is empty. Tasks that have
        already reached ``max_attempts`` are transitioned to ``FAILED`` and
        skipped rather than dispatched beyond their attempt limit.
        """
        if self.pending_count() == 0:
            return None

        effective_peers = [
            replace(
                peer,
                running_tasks=(
                    peer.running_tasks + self._reservations[peer.peer_id]
                ),
            )
            for peer in peers
        ]

        selected = self._scheduler.select_peer(effective_peers)
        if selected is None:
            return None

        while True:
            task = self.dequeue_task()
            if task is None:
                return None

            if (
                task.is_terminal
                or task.status != TaskStatus.QUEUED
                or task.task_id in self._active_by_task
            ):
                continue

            if task.attempt_count >= task.max_attempts:
                task.transition_to(
                    TaskStatus.FAILED,
                    error="Attempt limit exhausted before dispatch.",
                )
                self._persist_task_status(task)
                continue

            task.transition_to(TaskStatus.ASSIGNING)
            task.attempt_count += 1

            assignment = Assignment(
                assignment_id=str(uuid4()),
                task_id=task.task_id,
                peer_id=selected.peer_id,
                attempt_number=task.attempt_count,
            )

            self._active[assignment.assignment_id] = assignment
            self._active_by_task[task.task_id] = assignment.assignment_id
            self._ack_deadlines[assignment.assignment_id] = (
                self._ack_clock() + self._ack_timeout
            )
            self._reservations[selected.peer_id] += 1
            return assignment

    async def dispatch_next(
        self,
        peers: Iterable[PeerInfo],
        transport: TaskTransportProtocol,
    ) -> Optional[Assignment]:
        """Create the next assignment and dispatch ``TASK_SUBMIT`` via transport.

        If ``transport.send_message`` fails, the unconfirmed assignment is
        rolled back (reservation released, attempt counter reverted, status
        restored to ``QUEUED``, and task requeued at the front) so pending work
        never silently disappears, then the transport exception is re-raised.
        """
        assignment = self.assign_next(peers)
        if assignment is None:
            return None

        task = self.get_task(assignment.task_id)
        message = assignment.to_task_submit_message(
            task,
            sender_id=self._node_id,
        )

        try:
            await transport.send_message(assignment.peer_id, message)
        except Exception as exc:
            self._rollback_unconfirmed_assignment(
                assignment,
                error=f"Dispatch failed: {exc}",
            )
            raise

        return assignment

    def _matches(
        self,
        assignment_id: str,
        peer_id: str,
    ) -> Optional[Assignment]:
        assignment = self._active.get(assignment_id)
        if assignment is None:
            return None
        if assignment.peer_id != peer_id:
            return None
        return assignment

    def acknowledge(
        self,
        assignment_id: str,
        peer_id: str,
    ) -> bool:
        """Handle worker acceptance ACK (``ASSIGNING -> ASSIGNED``).

        Acceptance ACK confirms peer receipt only; it does NOT complete the
        task or release capacity. Duplicate ACKs are idempotent and never
        regress ``RUNNING`` back to ``ASSIGNED``.
        """
        assignment = self._matches(assignment_id, peer_id)
        if assignment is None:
            return False

        task = self.get_task(assignment.task_id)
        if self._expire_task_if_due(task):
            return False
        if task.status == TaskStatus.ASSIGNING:
            ack_deadline = self._ack_deadlines.get(assignment_id)
            if ack_deadline is not None and self._ack_clock() >= ack_deadline:
                self.fail_assignment(
                    assignment_id,
                    peer_id,
                    "Assignment acknowledgement timed out.",
                )
                return False

            task.transition_to(TaskStatus.ASSIGNED)
            self._ack_deadlines.pop(assignment_id, None)
            return True

        if task.status in (TaskStatus.ASSIGNED, TaskStatus.RUNNING):
            self._ack_deadlines.pop(assignment_id, None)
            return True

        return False

    def mark_running(
        self,
        assignment_id: str,
        peer_id: str,
    ) -> bool:
        """Transition an acknowledged assignment to ``RUNNING``."""
        assignment = self._matches(assignment_id, peer_id)
        if assignment is None:
            return False

        task = self.get_task(assignment.task_id)
        if self._expire_task_if_due(task):
            return False
        if task.status == TaskStatus.ASSIGNED:
            task.transition_to(TaskStatus.RUNNING)
            return True

        return task.status == TaskStatus.RUNNING

    def complete(
        self,
        assignment_id: str,
        peer_id: str,
    ) -> bool:
        """Mark an active assignment ``COMPLETED`` and release reservation once."""
        assignment = self._matches(assignment_id, peer_id)
        if assignment is None:
            return False

        task = self.get_task(assignment.task_id)
        if self._expire_task_if_due(task):
            return False
        if task.status not in (TaskStatus.ASSIGNED, TaskStatus.RUNNING):
            return False

        task.transition_to(TaskStatus.COMPLETED)
        self._release_assignment(assignment_id)
        self._persist_task_status(task)
        return True

    def fail_assignment(
        self,
        assignment_id: str,
        peer_id: str,
        error: str,
    ) -> bool:
        """Handle a rejected, unacknowledged, or failed active assignment.

        Releases the peer reservation and invalidates ``assignment_id`` so late
        messages from the previous worker cannot mutate newer attempts. The
        task is marked ``EXPIRED`` if past deadline, ``FAILED`` if it reached
        ``max_attempts``, or transitioned through ``RETRY_WAIT -> QUEUED`` and
        re-enqueued for retry without incrementing ``attempt_count``.
        """
        assignment = self._matches(assignment_id, peer_id)
        if assignment is None:
            return False

        task = self.get_task(assignment.task_id)
        if task.status not in (
            TaskStatus.ASSIGNING,
            TaskStatus.ASSIGNED,
            TaskStatus.RUNNING,
        ):
            return False

        if not isinstance(error, str) or not error.strip():
            raise TaskValidationError(
                "Assignment failure reason must be a non-empty string."
            )

        reason = error.strip()
        self._release_assignment(assignment_id)
        task.last_error = reason

        if self._expire_task_if_due(task):
            return True

        if task.attempt_count >= task.max_attempts:
            task.transition_to(TaskStatus.FAILED, error=reason)
            self._persist_task_status(task)
            return True

        task.transition_to(TaskStatus.RETRY_WAIT, error=reason)
        self.requeue_retry_task(task.task_id)
        return True

    def process_ack_timeouts(self) -> int:
        """Fail and retry/exhaust any ``ASSIGNING`` assignments past ``ack_timeout``.

        Only unacknowledged ``ASSIGNING`` assignments expire here; acknowledged
        ``ASSIGNED`` or ``RUNNING`` assignments are never timed out by ACK
        timeout processing (worker execution timeouts are owned by Member 3).
        """
        now = self._ack_clock()
        processed = 0

        for assignment_id, ack_deadline in list(self._ack_deadlines.items()):
            if now < ack_deadline:
                continue

            assignment = self._active.get(assignment_id)
            if assignment is None:
                self._ack_deadlines.pop(assignment_id, None)
                continue

            task = self.get_task(assignment.task_id)
            if task.is_terminal:
                self._release_assignment(assignment_id)
                continue

            if task.status != TaskStatus.ASSIGNING:
                self._ack_deadlines.pop(assignment_id, None)
                continue

            handled = self.fail_assignment(
                assignment_id,
                assignment.peer_id,
                "Assignment acknowledgement timed out.",
            )
            if handled:
                processed += 1

        return processed

    def handle_message(self, message: Message) -> bool:
        """Protocol adapter boundary for inbound worker lifecycle messages.

        Supports ``TASK_ACK``, ``TASK_REJECT``, ``TASK_STARTED``,
        ``TASK_RESULT``, and ``TASK_FAILED`` using Member 4's shared
        ``Message`` schema.
        """
        if not isinstance(message, Message):
            raise TaskValidationError(
                "message must be an instance of meshweaver.models.message.Message."
            )

        assignment_id = message.payload.get("assignment_id")
        if not isinstance(assignment_id, str) or not assignment_id.strip():
            return False

        peer_id = message.sender_id
        if message.message_type == MessageType.TASK_ACK:
            return self.acknowledge(assignment_id, peer_id)
        if message.message_type == MessageType.TASK_STARTED:
            return self.mark_running(assignment_id, peer_id)
        if message.message_type == MessageType.TASK_RESULT:
            return self.complete(assignment_id, peer_id)
        if message.message_type == MessageType.TASK_REJECT:
            assignment = self._matches(assignment_id, peer_id)
            if assignment is None:
                return False
            task = self.get_task(assignment.task_id)
            if task.status != TaskStatus.ASSIGNING:
                return False
            raw_reason = message.payload.get("reason") or message.payload.get("error")
            reason = (
                raw_reason.strip()
                if isinstance(raw_reason, str) and raw_reason.strip()
                else "Assignment rejected by peer."
            )
            return self.fail_assignment(assignment_id, peer_id, reason)
        if message.message_type == MessageType.TASK_FAILED:
            raw_error = message.payload.get("error") or message.payload.get("reason")
            reason = (
                raw_error.strip()
                if isinstance(raw_error, str) and raw_error.strip()
                else "Task execution failed on peer."
            )
            return self.fail_assignment(assignment_id, peer_id, reason)

        return False

    def get_active_assignment(
        self,
        assignment_id: str,
    ) -> Optional[Assignment]:
        """Return an active assignment by ID, or None if not active."""
        return self._active.get(assignment_id)

    def has_active_assignment(self, task_id: str) -> bool:
        """Return True if ``task_id`` currently has an active in-flight assignment."""
        return task_id in self._active_by_task

    def get_active_assignment_for_task(
        self,
        task_id: str,
    ) -> Optional[Assignment]:
        """Return the active assignment for ``task_id``, or None."""
        assignment_id = self._active_by_task.get(task_id)
        if assignment_id is None:
            return None
        return self._active.get(assignment_id)

    def active_assignment_count(self) -> int:
        """Return count of currently active (uncompleted) assignments."""
        return len(self._active)

    def pending_ack_count(self) -> int:
        """Return count of active assignments still awaiting acceptance ACK."""
        return len(self._ack_deadlines)

    def reservation_count(self, peer_id: str) -> int:
        """Return number of local active reservations held for ``peer_id``."""
        return self._reservations.get(peer_id, 0)
