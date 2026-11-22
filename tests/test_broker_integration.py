
import unittest
import tempfile
from pathlib import Path

from meshweaver.broker.broker import Broker
from meshweaver.broker.models import TaskStatus
from meshweaver.broker.peers import PeerInfo
from meshweaver.core.errors import StorageError
from meshweaver.storage.database import Database


class TestBrokerIntegration(unittest.TestCase):

    def test_assign_task_to_eligible_peer(self):
        broker = Broker()

        peer = PeerInfo(
            peer_id="peer-1",
            cpu_usage=20,
            running_tasks=0,
            max_tasks=5,
            is_online=True,
        )

        task = broker.submit_task("addition", 10, 20)
        assignment = broker.assign_next_task([peer])

        self.assertIsNotNone(assignment)

        assigned_task, selected_peer = assignment

        self.assertEqual(assigned_task.task_id, task.task_id)
        self.assertEqual(selected_peer.peer_id, "peer-1")
        self.assertEqual(task.status, TaskStatus.ASSIGNED)
        self.assertEqual(broker.pending_count(), 0)

    def test_task_returns_to_queue_when_no_peer_available(self):
        broker = Broker()

        task = broker.submit_task("addition", 10, 20)

        peer = PeerInfo(
            peer_id="peer-1",
            cpu_usage=20,
            running_tasks=5,
            max_tasks=5,
            is_online=True,
        )

        assignment = broker.assign_next_task([peer])

        self.assertIsNone(assignment)
        self.assertEqual(task.status, TaskStatus.QUEUED)
        self.assertEqual(broker.pending_count(), 1)

    def test_assignment_is_persisted_in_database(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(str(Path(temp_dir) / "integration.db"))
            db.initialize()
            db.add_peer("peer-1", "127.0.0.1", 5001, "online")

            broker = Broker(database=db)

            peer = PeerInfo(
                peer_id="peer-1",
                cpu_usage=20,
                running_tasks=0,
                max_tasks=5,
                is_online=True,
            )

            task = broker.submit_task("addition", 10, 20)
            assignment = broker.assign_next_task([peer])

            self.assertIsNotNone(assignment)

            stored_task = db.get_task(task.task_id)

            self.assertEqual(
                stored_task["status"],
                TaskStatus.ASSIGNED.value,
            )
            self.assertEqual(
                stored_task["assigned_peer_id"],
                "peer-1",
            )

    def test_database_failure_restores_task_to_queue(self):
        class FailingDatabase:
            def add_task(self, **kwargs):
                pass

            def update_task(self, **kwargs):
                raise RuntimeError("Simulated database failure")

        broker = Broker(database=FailingDatabase())

        peer = PeerInfo(
            peer_id="peer-1",
            cpu_usage=20,
            running_tasks=0,
            max_tasks=5,
            is_online=True,
        )

        task = broker.submit_task("addition", 10, 20)

        with self.assertRaises(StorageError):
            broker.assign_next_task([peer])

        self.assertEqual(task.status, TaskStatus.QUEUED)
        self.assertEqual(broker.pending_count(), 1)

    def test_scheduler_failure_restores_task_to_queue(self):
        class FailingScheduler:
            def select_peer(self, peers):
                raise RuntimeError("Simulated scheduler failure")

        broker = Broker(task_scheduler=FailingScheduler())
        task = broker.submit_task("addition", 10, 20)

        with self.assertRaises(RuntimeError):
            broker.assign_next_task([])

        self.assertEqual(task.status, TaskStatus.QUEUED)
        self.assertEqual(broker.pending_count(), 1)


if __name__ == "__main__":
    unittest.main()
