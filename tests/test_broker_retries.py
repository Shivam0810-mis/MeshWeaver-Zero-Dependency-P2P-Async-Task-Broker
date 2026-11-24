"""Unit tests for Day 16 retry policy on unacknowledged and failed assignments.

Integration boundary note:
    Uses a deterministic fake monotonic clock (for ACK timeouts) and fake UTC
    clock (for task deadlines) to test broker retry policy in isolation without
    sleeping or real TCP sockets.
"""

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from meshweaver.broker import (
    DispatchBroker,
    PeerInfo,
    TaskStatus,
    TaskValidationError,
)
from meshweaver.models.enums import MessageType
from meshweaver.models.message import Message
from meshweaver.storage.database import Database


class FakeMonotonicClock:
    def __init__(self, initial: float = 100.0) -> None:
        self.now = initial

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeUtcClock:
    def __init__(self, initial: datetime) -> None:
        self.now = initial

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        self.now += timedelta(**kwargs)


class TestRetryPolicyForUnacknowledgedAndFailedAssignments(unittest.TestCase):
    def test_invalid_ack_timeout_and_clock_rejected(self):
        for bad_timeout in (0, -1, -0.5, float("nan"), float("inf"), True, "5"):
            with self.subTest(ack_timeout=bad_timeout):
                with self.assertRaises(TaskValidationError):
                    DispatchBroker(ack_timeout=bad_timeout)  # type: ignore[arg-type]

        with self.assertRaises(TypeError):
            DispatchBroker(ack_clock="not-callable")  # type: ignore[arg-type]

    def test_unacknowledged_assignment_times_out_requeues_and_invalidates_old_id(self):
        ack_clock = FakeMonotonicClock(100.0)
        broker = DispatchBroker(ack_timeout=5.0, ack_clock=ack_clock)
        task = broker.submit_task("compute", 10, 20)
        peers = [PeerInfo("peer-a", cpu_usage=10, running_tasks=0, max_tasks=1)]

        first_assignment = broker.assign_next(peers)
        self.assertIsNotNone(first_assignment)
        self.assertEqual(task.status, TaskStatus.ASSIGNING)
        self.assertEqual(task.attempt_count, 1)
        self.assertEqual(broker.reservation_count("peer-a"), 1)
        self.assertEqual(broker.pending_ack_count(), 1)

        # Before timeout expires, nothing is timed out.
        ack_clock.advance(4.9)
        self.assertEqual(broker.process_ack_timeouts(), 0)
        self.assertEqual(task.status, TaskStatus.ASSIGNING)
        self.assertEqual(broker.reservation_count("peer-a"), 1)

        # At timeout boundary (t=105.0), assignment times out and task requeues.
        ack_clock.advance(0.1)
        self.assertEqual(broker.process_ack_timeouts(), 1)
        self.assertEqual(task.status, TaskStatus.QUEUED)
        self.assertEqual(task.attempt_count, 1)  # Requeue does not increment attempts.
        self.assertEqual(task.last_error, "Assignment acknowledgement timed out.")
        self.assertEqual(broker.pending_count(), 1)
        self.assertEqual(broker.reservation_count("peer-a"), 0)
        self.assertEqual(broker.active_assignment_count(), 0)
        self.assertEqual(broker.pending_ack_count(), 0)

        # Late ACK / start / complete for the invalidated assignment must be ignored.
        self.assertFalse(broker.acknowledge(first_assignment.assignment_id, "peer-a"))
        self.assertFalse(broker.mark_running(first_assignment.assignment_id, "peer-a"))
        self.assertFalse(broker.complete(first_assignment.assignment_id, "peer-a"))

        # Re-assigning creates a new assignment_id and increments attempt_count to 2.
        second_assignment = broker.assign_next(peers)
        self.assertIsNotNone(second_assignment)
        self.assertNotEqual(
            second_assignment.assignment_id,
            first_assignment.assignment_id,
        )
        self.assertEqual(second_assignment.attempt_number, 2)
        self.assertEqual(task.attempt_count, 2)

        self.assertTrue(broker.acknowledge(second_assignment.assignment_id, "peer-a"))
        self.assertTrue(broker.complete(second_assignment.assignment_id, "peer-a"))
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertEqual(broker.reservation_count("peer-a"), 0)

    def test_acknowledged_and_running_assignments_are_not_ack_timed_out(self):
        ack_clock = FakeMonotonicClock(10.0)
        broker = DispatchBroker(ack_timeout=5.0, ack_clock=ack_clock)
        task = broker.submit_task("long_job")
        peers = [PeerInfo("peer-a", cpu_usage=10, running_tasks=0, max_tasks=1)]

        assignment = broker.assign_next(peers)
        self.assertIsNotNone(assignment)
        self.assertTrue(broker.acknowledge(assignment.assignment_id, "peer-a"))
        self.assertEqual(broker.pending_ack_count(), 0)

        # Advancing far past ack_timeout must NOT time out an acknowledged/running task.
        ack_clock.advance(60.0)
        self.assertEqual(broker.process_ack_timeouts(), 0)
        self.assertEqual(task.status, TaskStatus.ASSIGNED)

        self.assertTrue(broker.mark_running(assignment.assignment_id, "peer-a"))
        ack_clock.advance(60.0)
        self.assertEqual(broker.process_ack_timeouts(), 0)
        self.assertEqual(task.status, TaskStatus.RUNNING)
        self.assertEqual(broker.reservation_count("peer-a"), 1)

    def test_retry_exhaustion_marks_task_failed_after_max_attempts(self):
        ack_clock = FakeMonotonicClock(0.0)
        broker = DispatchBroker(ack_timeout=5.0, ack_clock=ack_clock)
        task = broker.register_task("flaky_job", max_attempts=3)
        peers = [PeerInfo("peer-a", cpu_usage=10, running_tasks=0, max_tasks=1)]

        for expected_attempt in (1, 2):
            assignment = broker.assign_next(peers)
            self.assertIsNotNone(assignment)
            self.assertEqual(task.attempt_count, expected_attempt)
            ack_clock.advance(5.0)
            self.assertEqual(broker.process_ack_timeouts(), 1)
            self.assertEqual(task.status, TaskStatus.QUEUED)
            self.assertEqual(broker.pending_count(), 1)

        # 3rd and final attempt times out -> terminal FAILED (not requeued).
        final_assignment = broker.assign_next(peers)
        self.assertIsNotNone(final_assignment)
        self.assertEqual(task.attempt_count, 3)
        ack_clock.advance(5.0)
        self.assertEqual(broker.process_ack_timeouts(), 1)

        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertTrue(task.is_terminal)
        self.assertEqual(task.attempt_count, 3)
        self.assertEqual(broker.pending_count(), 0)
        self.assertEqual(broker.reservation_count("peer-a"), 0)
        self.assertIsNone(broker.assign_next(peers))

    def test_protocol_task_reject_and_task_failed_trigger_retry_policy(self):
        broker = DispatchBroker()
        task = broker.register_task("worker_job", max_attempts=2)
        peers = [PeerInfo("peer-a", cpu_usage=10, running_tasks=0, max_tasks=1)]

        # Attempt 1 rejected before ACK via TASK_REJECT -> requeued.
        a1 = broker.assign_next(peers)
        self.assertIsNotNone(a1)
        reject_msg = Message(
            message_type=MessageType.TASK_REJECT,
            sender_id="peer-a",
            payload={"assignment_id": a1.assignment_id, "reason": "worker busy"},
        )
        self.assertTrue(broker.handle_message(reject_msg))
        self.assertEqual(task.status, TaskStatus.QUEUED)
        self.assertEqual(task.last_error, "worker busy")
        self.assertEqual(broker.reservation_count("peer-a"), 0)

        # Attempt 2 accepted, starts running, then fails via TASK_FAILED -> FAILED.
        a2 = broker.assign_next(peers)
        self.assertIsNotNone(a2)
        self.assertTrue(broker.acknowledge(a2.assignment_id, "peer-a"))
        self.assertTrue(broker.mark_running(a2.assignment_id, "peer-a"))

        # TASK_REJECT after ACK is ignored.
        late_reject = Message(
            message_type=MessageType.TASK_REJECT,
            sender_id="peer-a",
            payload={"assignment_id": a2.assignment_id, "reason": "too late"},
        )
        self.assertFalse(broker.handle_message(late_reject))
        self.assertEqual(task.status, TaskStatus.RUNNING)

        fail_msg = Message(
            message_type=MessageType.TASK_FAILED,
            sender_id="peer-a",
            payload={"assignment_id": a2.assignment_id, "error": "division by zero"},
        )
        self.assertTrue(broker.handle_message(fail_msg))
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(task.last_error, "division by zero")
        self.assertEqual(broker.pending_count(), 0)
        self.assertEqual(broker.reservation_count("peer-a"), 0)

    def test_fail_assignment_validation_and_expired_deadline_stops_retry(self):
        utc_clock = FakeUtcClock(datetime(2026, 11, 24, 12, 0, 0, tzinfo=timezone.utc))
        ack_clock = FakeMonotonicClock(0.0)
        broker = DispatchBroker(
            clock=utc_clock,
            ack_timeout=5.0,
            ack_clock=ack_clock,
        )
        task = broker.submit_task_with_deadline(
            utc_clock.now + timedelta(seconds=10),
            "bounded_job",
        )
        peers = [PeerInfo("peer-a", cpu_usage=10, running_tasks=0, max_tasks=1)]

        assignment = broker.assign_next(peers)
        self.assertIsNotNone(assignment)

        # Wrong peer or empty failure reason rejected.
        self.assertFalse(broker.fail_assignment(assignment.assignment_id, "peer-b", "err"))
        with self.assertRaises(TaskValidationError):
            broker.fail_assignment(assignment.assignment_id, "peer-a", "   ")

        # Advance UTC deadline past expiry, then ACK times out -> EXPIRED (no retry).
        utc_clock.advance(seconds=15)
        ack_clock.advance(5.0)
        self.assertEqual(broker.process_ack_timeouts(), 1)
        self.assertEqual(task.status, TaskStatus.EXPIRED)
        self.assertEqual(broker.pending_count(), 0)
        self.assertEqual(broker.reservation_count("peer-a"), 0)

    def test_retry_exhaustion_persists_failed_status_in_sqlite(self):
        ack_clock = FakeMonotonicClock(0.0)
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(str(Path(temp_dir) / "retry_test.db"))
            db.initialize()
            broker = DispatchBroker(
                database=db,
                ack_timeout=5.0,
                ack_clock=ack_clock,
            )
            task = broker.register_task("single_try", max_attempts=1)
            peers = [PeerInfo("peer-a", cpu_usage=10, running_tasks=0, max_tasks=1)]

            assignment = broker.assign_next(peers)
            self.assertIsNotNone(assignment)
            ack_clock.advance(5.0)
            self.assertEqual(broker.process_ack_timeouts(), 1)

            self.assertEqual(task.status, TaskStatus.FAILED)
            self.assertEqual(db.get_task(task.task_id)["status"], TaskStatus.FAILED.value)


if __name__ == "__main__":
    unittest.main()
