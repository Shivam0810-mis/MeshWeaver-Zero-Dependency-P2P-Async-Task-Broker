import asyncio
import json
import unittest
from datetime import datetime, timedelta, timezone

from meshweaver.broker import (
    Broker,
    BrokerProtocol,
    DispatchBroker,
    InMemoryTaskQueue,
    InvalidTaskTransitionError,
    PeerInfo,
    TaskNotFoundError,
    TaskQueueProtocol,
    TaskStatus,
    TaskValidationError,
)
from meshweaver.core.errors import TaskError


class TestInMemoryTaskQueue(unittest.IsolatedAsyncioTestCase):
    def test_fifo_order_peek_snapshot_and_requeue_front(self):
        queue = InMemoryTaskQueue()
        self.assertIsInstance(queue, TaskQueueProtocol)
        self.assertTrue(queue.is_empty())
        self.assertEqual(queue.size(), 0)
        self.assertIsNone(queue.peek())
        self.assertIsNone(queue.dequeue_nowait())

        queue.enqueue("t-1")
        queue.enqueue("t-2")
        self.assertEqual(queue.size(), 2)
        self.assertFalse(queue.is_empty())
        self.assertEqual(queue.peek(), "t-1")
        self.assertTrue(queue.contains("t-2"))
        self.assertEqual(queue.snapshot(), ("t-1", "t-2"))

        queue.requeue_front("t-0")
        self.assertEqual(queue.snapshot(), ("t-0", "t-1", "t-2"))
        self.assertEqual(queue.dequeue_nowait(), "t-0")
        self.assertEqual(queue.dequeue_nowait(), "t-1")
        self.assertEqual(queue.dequeue_nowait(), "t-2")
        self.assertIsNone(queue.dequeue_nowait())

    def test_invalid_task_ids_rejected(self):
        queue = InMemoryTaskQueue()
        with self.assertRaises(TaskValidationError):
            queue.enqueue("   ")
        with self.assertRaises(TaskValidationError):
            queue.requeue_front("")

    async def test_async_dequeue_waits_for_enqueue(self):
        queue = InMemoryTaskQueue()
        consumer = asyncio.create_task(queue.dequeue())
        await asyncio.sleep(0)
        self.assertFalse(consumer.done())

        queue.enqueue("task-async-1")
        result = await consumer
        self.assertEqual(result, "task-async-1")
        self.assertTrue(queue.is_empty())

    async def test_cancelled_waiter_does_not_drop_item(self):
        queue = InMemoryTaskQueue()
        consumer = asyncio.create_task(queue.dequeue())
        await asyncio.sleep(0)

        # Enqueue resolves the waiter future, then cancel arrives before resume.
        queue.enqueue("task-keep")
        consumer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await consumer

        self.assertEqual(queue.size(), 1)
        self.assertEqual(queue.dequeue_nowait(), "task-keep")


class TestBrokerSubmissionValidationAndLookup(unittest.IsolatedAsyncioTestCase):
    def test_broker_conforms_to_protocol_and_accepts_json_payloads(self):
        broker = Broker()
        self.assertIsInstance(broker, BrokerProtocol)

        task = broker.submit_task(
            "  process_batch  ",
            1,
            2.5,
            True,
            None,
            ["nested", {"k": "v"}],
            options={"retries": 2, "flags": [False, 3]},
        )
        self.assertEqual(task.name, "process_batch")
        self.assertEqual(broker.task_count(), 1)
        self.assertEqual(broker.pending_count(), 1)

    def test_rejects_non_json_and_invalid_submissions(self):
        broker = Broker()

        with self.assertRaises(TaskValidationError):
            broker.submit_task("   ")

        with self.assertRaises(TaskValidationError):
            broker.submit_task("fn", object())

        with self.assertRaises(TaskValidationError):
            broker.submit_task("fn", {1: "non-string-key"})

        with self.assertRaises(TaskValidationError):
            broker.submit_task("fn", float("nan"))

        with self.assertRaises(TaskValidationError):
            broker.submit_task("fn", float("inf"))

        with self.assertRaises(TaskValidationError):
            broker.submit_task("fn", b"raw-bytes")

        with self.assertRaises(TaskValidationError):
            broker.register_task("fn", max_attempts=0)

        with self.assertRaises(TaskValidationError):
            broker.register_task("fn", max_attempts=True)

        self.assertEqual(broker.task_count(), 0)
        self.assertEqual(broker.pending_count(), 0)

    def test_submission_key_deduplication_and_conflict_rejection(self):
        broker = Broker()

        t1 = broker.submit_task_once(" req-1 ", "add", 10, 20, scale=2)
        t1_dup = broker.submit_task_once("req-1", "add", 10, 20, scale=2)
        self.assertEqual(t1.task_id, t1_dup.task_id)
        self.assertEqual(broker.task_count(), 1)
        self.assertEqual(broker.pending_count(), 1)

        # Different key can submit identical work.
        t2 = broker.submit_task_once("req-2", "add", 10, 20, scale=2)
        self.assertNotEqual(t1.task_id, t2.task_id)
        self.assertEqual(broker.task_count(), 2)
        self.assertEqual(broker.pending_count(), 2)

        # Option A: same key with conflicting payload raises TaskValidationError.
        with self.assertRaises(TaskValidationError):
            broker.submit_task_once("req-1", "add", 10, 99, scale=2)

        with self.assertRaises(TaskValidationError):
            broker.submit_task_once("req-1", "multiply", 10, 20, scale=2)

        # Dequeueing does not allow duplicate enqueue on repeat key.
        popped = broker.dequeue_task()
        self.assertIsNotNone(popped)
        self.assertEqual(popped.task_id, t1.task_id)
        self.assertEqual(broker.pending_count(), 1)

        again = broker.submit_task_once("req-1", "add", 10, 20, scale=2)
        self.assertEqual(again.task_id, t1.task_id)
        self.assertEqual(broker.pending_count(), 1)

    def test_lookup_status_summary_and_list_filtering(self):
        broker = Broker()
        t1 = broker.submit_task("job_a", 1)
        t2 = broker.submit_task("job_b", 2)
        t2.transition_to(TaskStatus.ASSIGNING)

        self.assertTrue(broker.has_task(t1.task_id))
        self.assertFalse(broker.has_task("missing-id"))
        self.assertEqual(broker.get_task_status(t1.task_id), TaskStatus.QUEUED)
        self.assertEqual(
            broker.get_task_status(t2.task_id), TaskStatus.ASSIGNING
        )

        summary = broker.get_task_summary(t1.task_id)
        self.assertEqual(json.loads(json.dumps(summary))["name"], "job_a")

        self.assertEqual(len(broker.list_tasks()), 2)
        self.assertEqual(
            [t.task_id for t in broker.list_tasks(TaskStatus.QUEUED)],
            [t1.task_id],
        )
        self.assertEqual(
            [t.task_id for t in broker.list_tasks(TaskStatus.ASSIGNING)],
            [t2.task_id],
        )

        with self.assertRaises(TaskNotFoundError):
            broker.get_task("missing-id")
        with self.assertRaises(KeyError):
            broker.get_task_status("missing-id")
        with self.assertRaises(TaskError):
            broker.get_task_summary("missing-id")
        with self.assertRaises(TaskValidationError):
            broker.list_tasks("QUEUED")  # type: ignore[arg-type]

    def test_requeue_task_front_and_deduplication(self):
        broker = Broker()
        t1 = broker.submit_task("first")
        t2 = broker.submit_task("second")

        dequeued = broker.dequeue_task()
        self.assertIsNotNone(dequeued)
        self.assertEqual(dequeued.task_id, t1.task_id)
        self.assertEqual(broker.pending_count(), 1)

        broker.requeue_task(t1.task_id, front=True)
        # Requeueing an already-queued task does not duplicate it.
        broker.requeue_task(t1.task_id, front=True)
        self.assertEqual(broker.pending_count(), 2)
        self.assertEqual(broker.dequeue_task().task_id, t1.task_id)
        self.assertEqual(broker.dequeue_task().task_id, t2.task_id)

        t1.transition_to(TaskStatus.ASSIGNING)
        with self.assertRaises(TaskValidationError):
            broker.requeue_task(t1.task_id)

    async def test_wait_for_task_returns_submitted_task(self):
        broker = Broker()
        waiter = asyncio.create_task(broker.wait_for_task())
        await asyncio.sleep(0)

        submitted = broker.submit_task("delayed_job", 42)
        received = await waiter
        self.assertEqual(received.task_id, submitted.task_id)


class TestTaskLifecycleTransitions(unittest.TestCase):
    def test_happy_path_and_retry_cycle(self):
        broker = Broker()
        task = broker.submit_task("etl_step", "chunk-1")
        self.assertEqual(task.status, TaskStatus.QUEUED)

        task.transition_to(TaskStatus.ASSIGNING)
        task.transition_to(TaskStatus.QUEUED, error="transport send failed")
        self.assertEqual(task.last_error, "transport send failed")

        task.transition_to(TaskStatus.ASSIGNING)
        task.transition_to(TaskStatus.ASSIGNED)
        task.transition_to(TaskStatus.RUNNING)
        task.transition_to(TaskStatus.RETRY_WAIT, error="transient error")
        task.transition_to(TaskStatus.QUEUED)
        task.transition_to(TaskStatus.ASSIGNING)
        task.transition_to(TaskStatus.ASSIGNED)
        task.transition_to(TaskStatus.COMPLETED)

        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertTrue(task.is_terminal)

    def test_illegal_transitions_rejected(self):
        broker = Broker()
        task = broker.submit_task("job")

        for illegal in (
            TaskStatus.ASSIGNED,
            TaskStatus.RUNNING,
            TaskStatus.RETRY_WAIT,
            TaskStatus.COMPLETED,
        ):
            with self.assertRaises(InvalidTaskTransitionError):
                task.transition_to(illegal)

        task.transition_to(TaskStatus.ASSIGNING)
        for illegal in (TaskStatus.RUNNING, TaskStatus.COMPLETED):
            with self.assertRaises(InvalidTaskTransitionError):
                task.transition_to(illegal)

        task.transition_to(TaskStatus.EXPIRED)
        for status in TaskStatus:
            with self.assertRaises(InvalidTaskTransitionError):
                task.transition_to(status)


class TestBrokerLifecycleEdgeCases(unittest.TestCase):
    def test_empty_and_whitespace_names_and_submission_keys_rejected(self):
        broker = Broker()

        for bad_name in ("", "   ", "\t\n"):
            with self.assertRaises(ValueError):
                broker.submit_task(bad_name, 1, 2)

        for bad_key in ("", "   ", "\n"):
            with self.assertRaises(ValueError):
                broker.submit_task_once(bad_key, "add", 1, 2)

        self.assertEqual(broker.task_count(), 0)
        self.assertEqual(broker.pending_count(), 0)

    def test_dequeue_empty_and_drained_queue_returns_none(self):
        broker = Broker()
        self.assertIsNone(broker.dequeue_task())

        task = broker.submit_task("single_job", 1)
        self.assertIs(broker.dequeue_task(), task)
        self.assertEqual(broker.pending_count(), 0)
        self.assertIsNone(broker.dequeue_task())

    def test_requeue_edge_cases_back_unknown_and_terminal_tasks(self):
        broker = Broker()
        t1 = broker.submit_task("first")
        t2 = broker.submit_task("second")

        self.assertIs(broker.dequeue_task(), t1)
        broker.requeue_task(t1.task_id, front=False)
        self.assertIs(broker.dequeue_task(), t2)
        self.assertIs(broker.dequeue_task(), t1)

        with self.assertRaises(TaskNotFoundError):
            broker.requeue_task("unknown-task-id")

        for terminal_status in (
            TaskStatus.EXPIRED,
            TaskStatus.FAILED,
        ):
            t = broker.submit_task(f"term_{terminal_status.value}")
            broker.dequeue_task()
            t.transition_to(terminal_status)
            with self.assertRaises(TaskValidationError):
                broker.requeue_task(t.task_id)

    def test_assign_next_task_skips_expired_and_handles_empty_queue(self):
        now = datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc)
        broker = Broker(clock=lambda: now)
        peer = PeerInfo("peer-1", cpu_usage=15, running_tasks=0, max_tasks=2)

        self.assertIsNone(broker.assign_next_task([peer]))

        expired = broker.submit_task_with_deadline(
            now - timedelta(seconds=1),
            "expired_job",
        )
        self.assertEqual(expired.status, TaskStatus.EXPIRED)
        self.assertIsNone(broker.assign_next_task([peer]))

        valid = broker.submit_task_with_deadline(
            now + timedelta(minutes=5),
            "valid_job",
        )
        assigned = broker.assign_next_task([peer])
        self.assertIsNotNone(assigned)
        assigned_task, selected_peer = assigned
        self.assertEqual(assigned_task.task_id, valid.task_id)
        self.assertEqual(selected_peer.peer_id, "peer-1")
        self.assertEqual(valid.status, TaskStatus.ASSIGNED)

    def test_register_task_submission_key_detects_deadline_conflict(self):
        now = datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc)
        broker = Broker(clock=lambda: now)
        d1 = now + timedelta(minutes=5)
        d2 = now + timedelta(minutes=10)

        t1 = broker.register_task("job", args=(1,), deadline_at=d1, submission_key="k-dl")
        t1_same = broker.register_task("job", args=(1,), deadline_at=d1, submission_key="k-dl")
        self.assertIs(t1, t1_same)

        with self.assertRaises(TaskValidationError):
            broker.register_task("job", args=(1,), deadline_at=d2, submission_key="k-dl")

    def test_dispatch_broker_ack_complete_and_multi_slot_reservations(self):
        broker = DispatchBroker()
        t1 = broker.submit_task("job_1")
        t2 = broker.submit_task("job_2")
        peers = [PeerInfo("peer-1", cpu_usage=10, running_tasks=0, max_tasks=2)]

        a1 = broker.assign_next(peers)
        a2 = broker.assign_next(peers)
        self.assertIsNotNone(a1)
        self.assertIsNotNone(a2)
        self.assertEqual(broker.reservation_count("peer-1"), 2)

        # Cannot complete before ACK (while ASSIGNING).
        self.assertFalse(broker.complete(a1.assignment_id, "peer-1"))
        self.assertEqual(t1.status, TaskStatus.ASSIGNING)
        self.assertEqual(broker.reservation_count("peer-1"), 2)

        # Wrong peer or unknown assignment rejected for mark_running.
        self.assertTrue(broker.acknowledge(a1.assignment_id, "peer-1"))
        self.assertFalse(broker.mark_running(a1.assignment_id, "peer-other"))
        self.assertFalse(broker.mark_running("missing-assignment", "peer-1"))

        # Direct ASSIGNED -> COMPLETED without mark_running succeeds once.
        self.assertTrue(broker.complete(a1.assignment_id, "peer-1"))
        self.assertEqual(t1.status, TaskStatus.COMPLETED)
        self.assertEqual(broker.reservation_count("peer-1"), 1)

        # Second task completes via ASSIGNED -> RUNNING -> COMPLETED.
        self.assertTrue(broker.acknowledge(a2.assignment_id, "peer-1"))
        self.assertTrue(broker.mark_running(a2.assignment_id, "peer-1"))
        self.assertTrue(broker.complete(a2.assignment_id, "peer-1"))
        self.assertEqual(t2.status, TaskStatus.COMPLETED)
        self.assertEqual(broker.reservation_count("peer-1"), 0)


if __name__ == "__main__":
    unittest.main()
