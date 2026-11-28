"""Isolated unit tests for DispatchBroker, async dispatch, ACKs, and attempts.

Integration boundary note:
    These tests use a fake async transport (``FakeTransport``), in-memory
    ``PeerInfo`` snapshots, and a fake monotonic clock. They verify Member 2's
    broker logic and Member 4's shared ``Message`` contract in isolation; they
    are NOT a real TCP network integration test (Member 1 owns TCP networking
    and Member 3 owns worker execution).
"""

import json
import unittest
from typing import List, Optional, Tuple

from meshweaver.broker import (
    Assignment,
    DispatchBroker,
    LeastLoadedScheduler,
    PeerHealth,
    PeerInfo,
    TaskStatus,
    TaskTransportProtocol,
    TaskValidationError,
)
from meshweaver.models.enums import MessageType
from meshweaver.models.message import Message


class FakeClock:
    def __init__(self, initial: float = 500.0) -> None:
        self.now = initial

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeTransport:
    """In-memory fake implementing ``TaskTransportProtocol`` for unit tests."""

    def __init__(self, fail_with: Optional[Exception] = None) -> None:
        self.sent: List[Tuple[str, Message]] = []
        self.fail_with = fail_with

    async def send_message(self, peer_id: str, message: Message) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.sent.append((peer_id, message))


class TestDispatchBrokerAsyncAndAck(unittest.IsolatedAsyncioTestCase):
    async def test_dispatch_next_sends_shared_task_submit_message(self):
        transport = FakeTransport()
        self.assertIsInstance(transport, TaskTransportProtocol)

        broker = DispatchBroker(node_id="broker-1")
        task = broker.submit_task("add_numbers", 10, 20, scale=2)
        peers = [
            PeerInfo("peer-slow", cpu_usage=70, running_tasks=0, max_tasks=2),
            PeerInfo("peer-fast", cpu_usage=15, running_tasks=0, max_tasks=2),
        ]

        assignment = await broker.dispatch_next(peers, transport)

        self.assertIsNotNone(assignment)
        self.assertIsInstance(assignment, Assignment)
        self.assertEqual(assignment.peer_id, "peer-fast")
        self.assertEqual(assignment.task_id, task.task_id)
        self.assertEqual(assignment.attempt_number, 1)
        self.assertEqual(task.attempt_count, 1)
        self.assertEqual(task.status, TaskStatus.ASSIGNING)
        self.assertEqual(broker.reservation_count("peer-fast"), 1)
        self.assertEqual(broker.active_assignment_count(), 1)

        self.assertEqual(len(transport.sent), 1)
        target_peer, message = transport.sent[0]
        self.assertEqual(target_peer, "peer-fast")
        self.assertEqual(message.message_type, MessageType.TASK_SUBMIT)
        self.assertEqual(message.sender_id, "broker-1")

        # Verify NDJSON/JSON round-trip through Member 4's Message contract.
        round_tripped = Message.from_dict(json.loads(json.dumps(message.to_dict())))
        self.assertEqual(round_tripped.payload["assignment_id"], assignment.assignment_id)
        self.assertEqual(round_tripped.payload["task_id"], task.task_id)
        self.assertEqual(round_tripped.payload["name"], "add_numbers")
        self.assertEqual(round_tripped.payload["args"], [10, 20])
        self.assertEqual(round_tripped.payload["kwargs"], {"scale": 2})
        self.assertEqual(round_tripped.payload["attempt_number"], 1)
        self.assertEqual(round_tripped.payload["max_attempts"], 3)

    async def test_dispatch_failure_rolls_back_and_requeues_task_at_front(self):
        failing_transport = FakeTransport(fail_with=ConnectionError("socket closed"))
        broker = DispatchBroker(node_id="broker-1")

        first = broker.submit_task("first_job", 1)
        second = broker.submit_task("second_job", 2)
        peers = [PeerInfo("peer-a", cpu_usage=10, running_tasks=0, max_tasks=1)]

        with self.assertRaises(ConnectionError):
            await broker.dispatch_next(peers, failing_transport)

        # Task must not disappear; reservation, attempt_count, and status roll back.
        self.assertEqual(first.status, TaskStatus.QUEUED)
        self.assertEqual(first.attempt_count, 0)
        self.assertIn("socket closed", first.last_error or "")
        self.assertEqual(broker.pending_count(), 2)
        self.assertEqual(broker.reservation_count("peer-a"), 0)
        self.assertEqual(broker.active_assignment_count(), 0)

        # Once transport recovers, the rolled-back first task dispatches first.
        healthy_transport = FakeTransport()
        assignment = await broker.dispatch_next(peers, healthy_transport)
        self.assertIsNotNone(assignment)
        self.assertEqual(assignment.task_id, first.task_id)
        self.assertEqual(first.attempt_count, 1)
        self.assertEqual(broker.pending_count(), 1)
        self.assertEqual(broker.get_task_status(second.task_id), TaskStatus.QUEUED)

    async def test_protocol_ack_started_and_result_lifecycle(self):
        transport = FakeTransport()
        broker = DispatchBroker(node_id="broker-1")
        task = broker.submit_task("compute", 5)
        peers = [PeerInfo("peer-a", cpu_usage=10, running_tasks=0, max_tasks=1)]

        assignment = await broker.dispatch_next(peers, transport)
        self.assertIsNotNone(assignment)

        # Wrong-peer ACK rejected.
        wrong_ack = Message(
            message_type=MessageType.TASK_ACK,
            sender_id="peer-other",
            payload={"assignment_id": assignment.assignment_id},
        )
        self.assertFalse(broker.handle_message(wrong_ack))
        self.assertEqual(task.status, TaskStatus.ASSIGNING)

        # TASK_STARTED before ACK is rejected.
        early_started = Message(
            message_type=MessageType.TASK_STARTED,
            sender_id="peer-a",
            payload={"assignment_id": assignment.assignment_id},
        )
        self.assertFalse(broker.handle_message(early_started))
        self.assertEqual(task.status, TaskStatus.ASSIGNING)

        # Valid TASK_ACK accepts assignment but does NOT complete it or release capacity.
        valid_ack = Message(
            message_type=MessageType.TASK_ACK,
            sender_id="peer-a",
            payload={"assignment_id": assignment.assignment_id},
        )
        self.assertTrue(broker.handle_message(valid_ack))
        self.assertEqual(task.status, TaskStatus.ASSIGNED)
        self.assertEqual(task.attempt_count, 1)
        self.assertEqual(broker.reservation_count("peer-a"), 1)

        # Duplicate ACK is idempotent.
        self.assertTrue(broker.handle_message(valid_ack))
        self.assertEqual(task.status, TaskStatus.ASSIGNED)
        self.assertEqual(broker.reservation_count("peer-a"), 1)

        # TASK_STARTED moves ASSIGNED -> RUNNING; late duplicate ACK must not regress.
        self.assertTrue(broker.handle_message(early_started))
        self.assertEqual(task.status, TaskStatus.RUNNING)
        self.assertTrue(broker.handle_message(valid_ack))
        self.assertEqual(task.status, TaskStatus.RUNNING)

        # TASK_RESULT completes task and releases reservation exactly once.
        result_msg = Message(
            message_type=MessageType.TASK_RESULT,
            sender_id="peer-a",
            payload={"assignment_id": assignment.assignment_id, "result": 25},
        )
        self.assertTrue(broker.handle_message(result_msg))
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertEqual(task.attempt_count, 1)
        self.assertEqual(broker.reservation_count("peer-a"), 0)
        self.assertEqual(broker.active_assignment_count(), 0)

        # Duplicate completion and late ACK after completion are rejected.
        self.assertFalse(broker.handle_message(result_msg))
        self.assertFalse(broker.handle_message(valid_ack))
        self.assertEqual(broker.reservation_count("peer-a"), 0)

    async def test_rejects_malformed_and_wrong_peer_task_results(self):
        transport = FakeTransport()
        broker = DispatchBroker(node_id="broker-1")
        task = broker.submit_task("compute", 5)
        peers = [PeerInfo("peer-a", cpu_usage=10, running_tasks=0, max_tasks=1)]

        assignment = await broker.dispatch_next(peers, transport)
        self.assertIsNotNone(assignment)

        valid_ack = Message(
            message_type=MessageType.TASK_ACK,
            sender_id="peer-a",
            payload={"assignment_id": assignment.assignment_id},
        )
        self.assertTrue(broker.handle_message(valid_ack))

        # Missing assignment_id is rejected.
        missing_id = Message(
            message_type=MessageType.TASK_RESULT,
            sender_id="peer-a",
            payload={"result": 25},
        )
        self.assertFalse(broker.handle_message(missing_id))

        # Empty assignment_id is rejected.
        empty_id = Message(
            message_type=MessageType.TASK_RESULT,
            sender_id="peer-a",
            payload={"assignment_id": " ", "result": 25},
        )
        self.assertFalse(broker.handle_message(empty_id))

        # Non-string assignment_id is rejected.
        invalid_id = Message(
            message_type=MessageType.TASK_RESULT,
            sender_id="peer-a",
            payload={"assignment_id": 123, "result": 25},
        )
        self.assertFalse(broker.handle_message(invalid_id))

        # Unknown assignment_id is rejected.
        unknown_id = Message(
            message_type=MessageType.TASK_RESULT,
            sender_id="peer-a",
            payload={"assignment_id": "unknown-assignment", "result": 25},
        )
        self.assertFalse(broker.handle_message(unknown_id))

        # A different peer cannot complete this assignment.
        wrong_peer = Message(
            message_type=MessageType.TASK_RESULT,
            sender_id="peer-other",
            payload={"assignment_id": assignment.assignment_id, "result": 25},
        )
        self.assertFalse(broker.handle_message(wrong_peer))

        # Rejected messages must not change task or reservation state.
        self.assertEqual(task.status, TaskStatus.ASSIGNED)
        self.assertEqual(broker.reservation_count("peer-a"), 1)
        self.assertEqual(broker.active_assignment_count(), 1)

    async def test_skips_stale_and_unhealthy_peers_with_fake_clock(self):
        clock = FakeClock(initial=200.0)
        scheduler = LeastLoadedScheduler(
            max_staleness_seconds=30.0,
            require_heartbeat=True,
            clock=clock,
        )
        broker = DispatchBroker(node_id="broker-1", scheduler=scheduler)
        transport = FakeTransport()

        task = broker.submit_task("job_1")
        peers = [
            PeerInfo(
                "peer-stale",
                cpu_usage=5,
                running_tasks=0,
                max_tasks=2,
                last_seen_monotonic=150.0,
            ),
            PeerInfo(
                "peer-unhealthy",
                cpu_usage=5,
                running_tasks=0,
                max_tasks=2,
                health=PeerHealth.UNHEALTHY,
                last_seen_monotonic=200.0,
            ),
            PeerInfo(
                "peer-healthy",
                cpu_usage=40,
                running_tasks=0,
                max_tasks=2,
                last_seen_monotonic=195.0,
            ),
        ]

        assignment = await broker.dispatch_next(peers, transport)
        self.assertIsNotNone(assignment)
        self.assertEqual(assignment.peer_id, "peer-healthy")
        self.assertEqual(task.attempt_count, 1)

        # Advance clock so peer-healthy also becomes stale; second task stays queued.
        second = broker.submit_task("job_2")
        clock.advance(40.0)
        self.assertIsNone(await broker.dispatch_next(peers, transport))
        self.assertEqual(second.status, TaskStatus.QUEUED)
        self.assertEqual(second.attempt_count, 0)
        self.assertEqual(broker.pending_count(), 1)

    async def test_exhausted_task_is_failed_and_not_assigned_past_max_attempts(self):
        broker = DispatchBroker(node_id="broker-1")
        transport = FakeTransport()

        exhausted = broker.register_task("already_exhausted", max_attempts=2)
        exhausted.attempt_count = 2
        fresh = broker.register_task("fresh_job", max_attempts=2)

        peers = [PeerInfo("peer-a", cpu_usage=10, running_tasks=0, max_tasks=1)]
        assignment = await broker.dispatch_next(peers, transport)

        self.assertEqual(exhausted.status, TaskStatus.FAILED)
        self.assertEqual(exhausted.attempt_count, 2)
        self.assertIsNotNone(exhausted.last_error)

        self.assertIsNotNone(assignment)
        self.assertEqual(assignment.task_id, fresh.task_id)
        self.assertEqual(fresh.attempt_count, 1)
        self.assertEqual(len(transport.sent), 1)

    async def test_empty_queue_and_invalid_message_handling(self):
        broker = DispatchBroker(node_id="broker-1")
        transport = FakeTransport()
        peers = [PeerInfo("peer-a", cpu_usage=10, running_tasks=0, max_tasks=1)]

        self.assertIsNone(await broker.dispatch_next(peers, transport))
        self.assertEqual(len(transport.sent), 0)
        self.assertEqual(broker.reservation_count("peer-a"), 0)

        with self.assertRaises(TaskValidationError):
            broker.handle_message({"not": "a-message"})  # type: ignore[arg-type]

        unsupported = Message(
            message_type=MessageType.HEARTBEAT,
            sender_id="peer-a",
            payload={"assignment_id": "a-1"},
        )
        self.assertFalse(broker.handle_message(unsupported))


if __name__ == "__main__":
    unittest.main()
