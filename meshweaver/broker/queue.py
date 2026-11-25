"""In-memory pending-task queue abstraction for the MeshWeaver broker.

Unlike ``asyncio.Queue`` with immediate ``task_done()`` on dequeue, this queue
only tracks pending task IDs waiting to be dispatched and does not conflate
dequeueing with task execution completion.
"""

import asyncio
from collections import deque
from typing import Deque, Optional, Tuple

from .models import TaskValidationError


class InMemoryTaskQueue:
    """FIFO in-memory queue for pending task IDs with safe front-requeue."""

    def __init__(self) -> None:
        self._items: Deque[str] = deque()
        self._waiters: Deque[asyncio.Future[str]] = deque()

    def _validate_task_id(self, task_id: str) -> str:
        if not isinstance(task_id, str) or not task_id.strip():
            raise TaskValidationError("task_id must be a non-empty string.")
        return task_id.strip()

    def _handoff_to_waiter(self, task_id: str) -> bool:
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                waiter.set_result(task_id)
                return True
        return False

    def enqueue(self, task_id: str) -> None:
        """Append a pending task ID to the back of the queue."""
        normalized_id = self._validate_task_id(task_id)
        if not self._handoff_to_waiter(normalized_id):
            self._items.append(normalized_id)

    def requeue_front(self, task_id: str) -> None:
        """Put a task ID back at the front (e.g., on dispatch failure)."""
        normalized_id = self._validate_task_id(task_id)
        if not self._handoff_to_waiter(normalized_id):
            self._items.appendleft(normalized_id)

    def dequeue_nowait(self) -> Optional[str]:
        """Pop the next pending task ID without blocking, or None if empty."""
        if not self._items:
            return None
        return self._items.popleft()

    async def dequeue(self) -> str:
        """Await and return the next pending task ID without losing items on cancel."""
        if self._items:
            return self._items.popleft()

        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[str] = loop.create_future()
        self._waiters.append(waiter)
        try:
            return await waiter
        except BaseException:
            if waiter in self._waiters:
                self._waiters.remove(waiter)
            if waiter.done() and not waiter.cancelled():
                self.requeue_front(waiter.result())
            raise

    def peek(self) -> Optional[str]:
        """Return the next pending task ID without removing it, or None."""
        if not self._items:
            return None
        return self._items[0]

    def contains(self, task_id: str) -> bool:
        """Return True if ``task_id`` is currently waiting in the queue."""
        return task_id in self._items

    def snapshot(self) -> Tuple[str, ...]:
        """Return an immutable snapshot of queued task IDs in FIFO order."""
        return tuple(self._items)

    def size(self) -> int:
        """Return the number of currently queued task IDs."""
        return len(self._items)

    def is_empty(self) -> bool:
        """Return True when no task IDs are waiting in the queue."""
        return not self._items
