import asyncio
import json
import unittest

from meshweaver.broker import (
    Broker,
    BrokerProtocol,
    InMemoryTaskQueue,
    InvalidTaskTransitionError,
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


if __name__ == "__main__":
    unittest.main()
