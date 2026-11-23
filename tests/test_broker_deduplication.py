"""Unit tests for Day 15 idempotency keys and duplicate task execution prevention."""

import unittest
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import Mock

from meshweaver.broker import (
    Broker,
    DispatchBroker,
    PeerInfo,
    TaskStatus,
    TaskValidationError,
)
from meshweaver.core.errors import StorageError


class TestBrokerDeduplicationAndIdempotency(unittest.TestCase):
    def test_same_key_and_payload_creates_only_one_queue_entry(self):
        broker = Broker()

        first = broker.submit_task_once(
            "order-123", "add", 1, 2, label="total"
        )
        second = broker.submit_task_once(
            "order-123", "add", 1, 2, label="total"
        )

        self.assertIs(first, second)
        self.assertTrue(broker.has_submission_key("order-123"))
        self.assertTrue(broker.has_submission_key("  order-123  "))
        self.assertFalse(broker.has_submission_key("unknown-key"))
        self.assertIs(broker.get_task_by_submission_key("order-123"), first)
        self.assertIsNone(broker.get_task_by_submission_key("unknown-key"))
        self.assertEqual(broker.task_count(), 1)
        self.assertEqual(broker.pending_count(), 1)

        self.assertIs(broker.dequeue_task(), first)
        self.assertIsNone(broker.dequeue_task())

    def test_different_keys_create_separate_tasks(self):
        broker = Broker()

        first = broker.submit_task_once("order-1", "add", 1, 2)
        second = broker.submit_task_once("order-2", "add", 1, 2)

        self.assertIsNot(first, second)
        self.assertNotEqual(first.task_id, second.task_id)
        self.assertEqual(broker.task_count(), 2)
        self.assertEqual(broker.pending_count(), 2)

        self.assertIs(broker.dequeue_task(), first)
        self.assertIs(broker.dequeue_task(), second)
        self.assertIsNone(broker.dequeue_task())

    def test_duplicate_after_dequeue_does_not_requeue(self):
        broker = Broker()

        original = broker.submit_task_once("order-123", "add", 1, 2)
        self.assertIs(broker.dequeue_task(), original)

        duplicate = broker.submit_task_once("order-123", "add", 1, 2)

        self.assertIs(duplicate, original)
        self.assertEqual(broker.task_count(), 1)
        self.assertEqual(broker.pending_count(), 0)
        self.assertIsNone(broker.dequeue_task())

    def test_duplicate_does_not_revive_terminal_task(self):
        for status in (
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.EXPIRED,
        ):
            with self.subTest(status=status):
                broker = Broker()
                original = broker.submit_task_once("order-123", "add", 1, 2)
                self.assertIs(broker.dequeue_task(), original)

                if status == TaskStatus.COMPLETED:
                    original.transition_to(TaskStatus.ASSIGNING)
                    original.transition_to(TaskStatus.ASSIGNED)
                original.transition_to(status)

                duplicate = broker.submit_task_once("order-123", "add", 1, 2)

                self.assertIs(duplicate, original)
                self.assertEqual(duplicate.status, status)
                self.assertEqual(broker.task_count(), 1)
                self.assertEqual(broker.pending_count(), 0)
                self.assertIsNone(broker.dequeue_task())

    def test_conflicting_payload_rejected_and_preserves_original_task(self):
        broker = Broker()

        original = broker.submit_task_once(
            "order-123", "add", 1, 2, label="original"
        )
        with self.assertRaises(TaskValidationError):
            broker.submit_task_once(
                "order-123", "multiply", 9, 10, label="replacement"
            )

        self.assertEqual(original.name, "add")
        self.assertEqual(original.args, (1, 2))
        self.assertEqual(original.kwargs, {"label": "original"})
        self.assertEqual(broker.task_count(), 1)
        self.assertEqual(broker.pending_count(), 1)

    def test_invalid_submission_keys_leave_broker_unchanged(self):
        invalid_keys: tuple[Any, ...] = ("", "   ", None, 123)
        for key in invalid_keys:
            with self.subTest(key=key):
                broker = Broker()
                self.assertFalse(broker.has_submission_key(key))
                self.assertIsNone(broker.get_task_by_submission_key(key))

                with self.assertRaises(ValueError):
                    broker.submit_task_once(key, "add", 1, 2)

                self.assertEqual(broker.task_count(), 0)
                self.assertEqual(broker.pending_count(), 0)
                self.assertIsNone(broker.dequeue_task())

    def test_failed_submission_does_not_reserve_key(self):
        broker = Broker()

        with self.assertRaises(TaskValidationError):
            broker.submit_task_once("order-123", "   ")

        with self.assertRaises(TaskValidationError):
            broker.submit_task_once("order-123", "add", object())

        self.assertFalse(broker.has_submission_key("order-123"))
        self.assertEqual(broker.task_count(), 0)
        self.assertEqual(broker.pending_count(), 0)

        task = broker.submit_task_once("order-123", "add", 1, 2)
        self.assertEqual(task.name, "add")
        self.assertTrue(broker.has_submission_key("order-123"))
        self.assertEqual(broker.task_count(), 1)
        self.assertEqual(broker.pending_count(), 1)
        self.assertIs(broker.dequeue_task(), task)

    def test_storage_failure_does_not_reserve_submission_key(self):
        failing_db = Mock()
        failing_db.add_task.side_effect = RuntimeError("db unavailable")

        broker = Broker(database=failing_db)

        with self.assertRaises(StorageError):
            broker.submit_task_once("order-db", "add", 1, 2)

        self.assertFalse(broker.has_submission_key("order-db"))
        self.assertEqual(broker.task_count(), 0)
        self.assertEqual(broker.pending_count(), 0)

        # Once storage recovers, retrying with the same key succeeds.
        failing_db.add_task.side_effect = None
        task = broker.submit_task_once("order-db", "add", 1, 2)
        self.assertTrue(broker.has_submission_key("order-db"))
        self.assertEqual(broker.task_count(), 1)
        self.assertEqual(broker.pending_count(), 1)
        self.assertIs(broker.get_task_by_submission_key("order-db"), task)

    def test_submit_task_once_with_deadline_deduplicates_and_checks_conflicts(self):
        now = datetime(2026, 11, 23, 12, 0, 0, tzinfo=timezone.utc)
        broker = Broker(clock=lambda: now)

        future = now + timedelta(minutes=5)
        first = broker.submit_task_once_with_deadline(
            "dl-key", future, "add", 1, 2
        )
        same = broker.submit_task_once_with_deadline(
            "dl-key", future, "add", 1, 2
        )
        self.assertIs(first, same)
        self.assertEqual(broker.pending_count(), 1)

        with self.assertRaises(TaskValidationError):
            broker.submit_task_once_with_deadline(
                "dl-key", future + timedelta(minutes=1), "add", 1, 2
            )

        past = now - timedelta(seconds=5)
        expired = broker.submit_task_once_with_deadline(
            "dl-expired", past, "add", 1, 2
        )
        self.assertEqual(expired.status, TaskStatus.EXPIRED)
        self.assertIs(
            broker.submit_task_once_with_deadline(
                "dl-expired", past, "add", 1, 2
            ),
            expired,
        )
        self.assertEqual(broker.pending_count(), 1)

    def test_dispatch_broker_prevents_duplicate_active_assignment(self):
        broker = DispatchBroker()
        task = broker.submit_task_once("unique-job", "add", 2, 3)
        peers = [PeerInfo("peer-1", cpu_usage=10, running_tasks=0, max_tasks=2)]

        assignment = broker.assign_next(peers)
        self.assertIsNotNone(assignment)
        self.assertTrue(broker.has_active_assignment(task.task_id))
        self.assertEqual(
            broker.get_active_assignment_for_task(task.task_id),
            assignment,
        )

        # Even if the same task_id were forcibly queued again while in flight,
        # DispatchBroker must skip it and never create a duplicate execution.
        broker._queue.enqueue(task.task_id)
        self.assertIsNone(broker.assign_next(peers))
        self.assertEqual(broker.active_assignment_count(), 1)
        self.assertEqual(broker.reservation_count("peer-1"), 1)
        self.assertEqual(task.attempt_count, 1)

        self.assertTrue(broker.acknowledge(assignment.assignment_id, "peer-1"))
        self.assertTrue(broker.complete(assignment.assignment_id, "peer-1"))
        self.assertFalse(broker.has_active_assignment(task.task_id))
        self.assertIsNone(broker.get_active_assignment_for_task(task.task_id))


if __name__ == "__main__":
    unittest.main()
