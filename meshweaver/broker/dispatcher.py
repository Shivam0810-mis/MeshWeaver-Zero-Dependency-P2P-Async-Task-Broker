from collections import Counter
from dataclasses import dataclass, replace
from typing import Dict, Iterable, Optional
from uuid import uuid4


from .broker import Broker
from .models import TaskStatus
from .peers import PeerInfo
from .scheduler import scheduler

@dataclass(frozen=True)
class Assignment:
    assignment_id: str
    task_id: str
    peer_id: str



class DispatchBroker(Broker):
    def __init__(self):
        super().__init__()
        self._scheduler = scheduler()
        self._reservations: Counter = Counter()
        self._active: Dict[str, Assignment] = {}

    def assign_next(
        self,
        peers: Iterable[PeerInfo],
    ) -> Optional[Assignment]:
        effective_peers = [
            replace(
                peer,
                running_tasks=(
                    peer.running_tasks
                    + self._reservations[peer.peer_id]
                ),
            )
            for peer in peers
        ]

        selected = self._scheduler.select_peer(effective_peers)

        if selected is None:
            return None

        task = self.dequeue_task()

        if task is None:
            return None

        assignment = Assignment(
            assignment_id=str(uuid4()),
            task_id=task.task_id,
            peer_id=selected.peer_id,
        )

        
        self._active[assignment.assignment_id] = assignment
        self._reservations[selected.peer_id] += 1

        task.attempt_count += 1
        task.status = TaskStatus.ASSIGNING

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
        assignment = self._matches(assignment_id, peer_id)

        if assignment is None:
            return False

        task = self.get_task(assignment.task_id)

        if task.status == TaskStatus.ASSIGNING:
            task.status = TaskStatus.ASSIGNED
            return True

        return task.status in (
            TaskStatus.ASSIGNED,
            TaskStatus.RUNNING,
        )

    def mark_running(
        self,
        assignment_id: str,
        peer_id: str,
    ) -> bool:
        assignment = self._matches(assignment_id, peer_id)

        if assignment is None:
            return False

        task = self.get_task(assignment.task_id)

        if task.status == TaskStatus.ASSIGNED:
            task.status = TaskStatus.RUNNING
            return True

        return task.status == TaskStatus.RUNNING

    def complete(
        self,
        assignment_id: str,
        peer_id: str,
    ) -> bool:
        assignment = self._matches(assignment_id, peer_id)

        if assignment is None:
            return False

        task = self.get_task(assignment.task_id)

        if task.status not in (
            TaskStatus.ASSIGNED,
            TaskStatus.RUNNING,
        ):
            return False

        task.status = TaskStatus.COMPLETED

        self._reservations[peer_id] -= 1
        del self._active[assignment_id]

        return True

    def reservation_count(self, peer_id: str) -> int:
        return self._reservations[peer_id]    
