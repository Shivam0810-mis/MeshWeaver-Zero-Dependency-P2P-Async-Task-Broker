"""Unit tests for Day 13 task deadlines and expiry enforcement."""

import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from meshweaver.broker import (
    Broker,
    DispatchBroker,
    PeerInfo,
    TaskStatus,
    TaskValidationError,
)
from meshweaver.storage.database import Database


class FakeUtcClock:
    def __init__(self, initial: datetime) -> None:
        self.now = initial

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        self.now += timedelta(**kwargs)


class TestTaskDeadlines(unittest.IsolatedAsyncioTestCase):
    def test_task_without_deadline_is_queued(self):
        broker = Broker()
        task = broker.submit_task("add", 1, 2)

        self.assertIsNone(task.deadline_at)
        self.assertEqual(task.status, TaskStatus.QUEUED)
        self.assertFalse(task.is_expired())
        self.assertEqual(broker.pending_count(), 1)
        self.assertIsNone(task.to_dict()["deadline_at"])

    def test_future_deadline_task_is_queued_once_and_normalized_to_utc(self):
        clock = FakeUtcClock(datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc))
        broker = Broker(clock=clock)

        ist = timezone(timedelta(hours=5, minutes=30))
        deadline_ist = datetime(2026, 11, 22, 17, 35, 0, tzinfo=ist)

        task = broker.submit_task_with_deadline(deadline_ist, "add", 1, 2)

        self.assertEqual(task.deadline_at, deadline_ist.astimezone(timezone.utc))
        self.assertEqual(task.status, TaskStatus.QUEUED)
        self.assertEqual(broker.pending_count(), 1)
        self.assertEqual(
            task.to_dict()["deadline_at"],
            deadline_ist.astimezone(timezone.utc).isoformat(),
        )

    def test_past_deadline_task_is_immediately_expired_and_not_enqueued(self):
        clock = FakeUtcClock(datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc))
        broker = Broker(clock=clock)

        past_deadline = clock.now - timedelta(seconds=1)
        task = broker.submit_task_with_deadline(past_deadline, "add", 1, 2)

        self.assertEqual(task.status, TaskStatus.EXPIRED)
        self.assertTrue(task.is_terminal)
        self.assertTrue(task.is_expired(clock.now))
        self.assertEqual(broker.task_count(), 1)
        self.assertEqual(broker.pending_count(), 0)
        self.assertIsNone(broker.dequeue_task())

    def test_naive_and_non_datetime_deadlines_are_rejected(self):
        broker = Broker()

        with self.assertRaises(ValueError):
            broker.submit_task_with_deadline(datetime.now(), "add", 1, 2)

        with self.assertRaises(TaskValidationError):
            broker.submit_task_with_deadline(
                "2026-11-22T12:00:00Z",  # type: ignore[arg-type]
                "add",
                1,
                2,
            )

        self.assertEqual(broker.task_count(), 0)
        self.assertEqual(broker.pending_count(), 0)

    def test_queued_task_expires_when_clock_passes_deadline_and_is_skipped(self):
        clock = FakeUtcClock(datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc))
        broker = Broker(clock=clock)

        short_task = broker.submit_task_with_deadline(
            clock.now + timedelta(seconds=10),
            "short_job",
            1,
        )
        long_task = broker.submit_task_with_deadline(
            clock.now + timedelta(minutes=5),
            "long_job",
            2,
        )
        self.assertEqual(broker.pending_count(), 2)

        clock.advance(seconds=10)

        dequeued = broker.dequeue_task()
        self.assertIsNotNone(dequeued)
        self.assertEqual(dequeued.task_id, long_task.task_id)
        self.assertEqual(short_task.status, TaskStatus.EXPIRED)
        self.assertEqual(long_task.status, TaskStatus.QUEUED)
        self.assertEqual(broker.pending_count(), 0)

    async def test_wait_for_task_skips_expired_task(self):
        clock = FakeUtcClock(datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc))
        broker = Broker(clock=clock)

        expiring = broker.submit_task_with_deadline(
            clock.now + timedelta(seconds=5),
            "expiring_job",
        )
        clock.advance(seconds=10)

        waiter = asyncio.create_task(broker.wait_for_task())
        await asyncio.sleep(0)

        fresh = broker.submit_task("fresh_job")
        received = await waiter

        self.assertEqual(expiring.status, TaskStatus.EXPIRED)
        self.assertEqual(received.task_id, fresh.task_id)

    def test_expire_overdue_tasks_and_dispatch_broker_skip(self):
        clock = FakeUtcClock(datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc))
        broker = DispatchBroker(clock=clock)

        t_expired = broker.submit_task_with_deadline(
            clock.now + timedelta(seconds=30),
            "expires_soon",
        )
        t_valid = broker.submit_task_with_deadline(
            clock.now + timedelta(minutes=10),
            "stays_valid",
        )

        clock.advance(seconds=31)
        peers = [PeerInfo("peer-1", cpu_usage=10, running_tasks=0, max_tasks=2)]

        assignment = broker.assign_next(peers)
        self.assertIsNotNone(assignment)
        self.assertEqual(assignment.task_id, t_valid.task_id)
        self.assertEqual(t_expired.status, TaskStatus.EXPIRED)
        self.assertEqual(t_expired.attempt_count, 0)

        clock.advance(minutes=15)
        newly_expired = broker.expire_overdue_tasks()
        self.assertEqual([t.task_id for t in newly_expired], [t_valid.task_id])
        self.assertEqual(t_valid.status, TaskStatus.EXPIRED)

    def test_deadline_expiry_persisted_with_sqlite_database(self):
        clock = FakeUtcClock(datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc))
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(str(Path(temp_dir) / "deadline_test.db"))
            db.initialize()
            broker = Broker(database=db, clock=clock)

            already_expired = broker.submit_task_with_deadline(
                clock.now - timedelta(seconds=5),
                "old_job",
            )
            self.assertEqual(
                db.get_task(already_expired.task_id)["status"],
                TaskStatus.EXPIRED.value,
            )

            will_expire = broker.submit_task_with_deadline(
                clock.now + timedelta(seconds=5),
                "later_job",
            )
            self.assertEqual(
                db.get_task(will_expire.task_id)["status"],
                TaskStatus.QUEUED.value,
            )

            clock.advance(seconds=10)
            self.assertIsNone(broker.dequeue_task())
            self.assertEqual(
                db.get_task(will_expire.task_id)["status"],
                TaskStatus.EXPIRED.value,
            )


if __name__ == "__main__":
    unittest.main()
