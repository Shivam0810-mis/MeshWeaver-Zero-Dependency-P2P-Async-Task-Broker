"""Unit tests for Day 17 broker expiry processing across active assignments."""

import unittest
from datetime import datetime, timedelta, timezone

from meshweaver.broker import DispatchBroker, PeerInfo, TaskStatus


class FakeUtcClock:
    def __init__(self, initial: datetime) -> None:
        self.now = initial

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        self.now += timedelta(**kwargs)


class TestExpiryProcessingWithAssignments(unittest.TestCase):
    def setUp(self):
        self.clock = FakeUtcClock(
            datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc)
        )
        self.broker = DispatchBroker(
            clock=self.clock,
            ack_clock=lambda: 0.0,
        )
        self.peers = [
            PeerInfo("peer-1", cpu_usage=10, running_tasks=0, max_tasks=2)
        ]

    def _submit_and_assign(self, name: str, seconds: int):
        task = self.broker.submit_task_with_deadline(
            self.clock.now + timedelta(seconds=seconds),
            name,
        )
        assignment = self.broker.assign_next(self.peers)
        self.assertIsNotNone(assignment)
        assert assignment is not None
        self.assertEqual(assignment.task_id, task.task_id)
        return task, assignment

    def _assert_released(self, task):
        self.assertEqual(task.status, TaskStatus.EXPIRED)
        self.assertEqual(self.broker.reservation_count("peer-1"), 0)
        self.assertEqual(self.broker.active_assignment_count(), 0)
        self.assertEqual(self.broker.pending_ack_count(), 0)
        self.assertFalse(self.broker.has_active_assignment(task.task_id))

    def test_expired_assigning_task_releases_assignment(self):
        task, assignment = self._submit_and_assign("job", 30)

        self.clock.advance(seconds=31)
        expired = self.broker.expire_overdue_tasks()

        self.assertEqual([t.task_id for t in expired], [task.task_id])
        self._assert_released(task)
        self.assertFalse(
            self.broker.acknowledge(assignment.assignment_id, "peer-1")
        )

    def test_expired_assigned_task_releases_assignment(self):
        task, assignment = self._submit_and_assign("job", 30)
        self.assertTrue(
            self.broker.acknowledge(assignment.assignment_id, "peer-1")
        )

        self.clock.advance(seconds=31)
        self.broker.expire_overdue_tasks()

        self._assert_released(task)
        self.assertFalse(
            self.broker.mark_running(assignment.assignment_id, "peer-1")
        )

    def test_expired_running_task_releases_and_rejects_late_result(self):
        task, assignment = self._submit_and_assign("job", 30)
        self.assertTrue(
            self.broker.acknowledge(assignment.assignment_id, "peer-1")
        )
        self.assertTrue(
            self.broker.mark_running(assignment.assignment_id, "peer-1")
        )

        self.clock.advance(seconds=31)
        self.broker.expire_overdue_tasks()

        self._assert_released(task)
        self.assertFalse(
            self.broker.complete(assignment.assignment_id, "peer-1")
        )
        self.assertEqual(task.status, TaskStatus.EXPIRED)

    def test_late_complete_before_sweep_expires_and_releases_assignment(self):
        task, assignment = self._submit_and_assign("job", 30)
        self.assertTrue(
            self.broker.acknowledge(assignment.assignment_id, "peer-1")
        )
        self.assertTrue(
            self.broker.mark_running(assignment.assignment_id, "peer-1")
        )

        self.clock.advance(seconds=31)
        self.assertFalse(
            self.broker.complete(assignment.assignment_id, "peer-1")
        )
        self._assert_released(task)

    def test_sweep_is_idempotent(self):
        task, _ = self._submit_and_assign("job", 30)

        self.clock.advance(seconds=31)
        self.assertEqual(len(self.broker.expire_overdue_tasks()), 1)
        self.assertEqual(self.broker.expire_overdue_tasks(), [])
        self._assert_released(task)

    def test_unexpired_assignment_keeps_its_reservation(self):
        short = self.broker.submit_task_with_deadline(
            self.clock.now + timedelta(seconds=30), "short_job"
        )
        long = self.broker.submit_task_with_deadline(
            self.clock.now + timedelta(minutes=10), "long_job"
        )
        first = self.broker.assign_next(self.peers)
        second = self.broker.assign_next(self.peers)
        assert first is not None and second is not None
        self.assertEqual(first.task_id, short.task_id)
        self.assertEqual(second.task_id, long.task_id)

        self.clock.advance(seconds=31)
        expired = self.broker.expire_overdue_tasks()

        self.assertEqual([t.task_id for t in expired], [short.task_id])
        self.assertEqual(short.status, TaskStatus.EXPIRED)
        self.assertEqual(long.status, TaskStatus.ASSIGNING)
        self.assertEqual(self.broker.reservation_count("peer-1"), 1)
        self.assertTrue(self.broker.has_active_assignment(long.task_id))
        self.assertTrue(
            self.broker.acknowledge(second.assignment_id, "peer-1")
        )


if __name__ == "__main__":
    unittest.main()
