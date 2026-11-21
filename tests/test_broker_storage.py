
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from meshweaver.broker.broker import Broker
from meshweaver.broker.models import TaskStatus
from meshweaver.broker.peers import PeerInfo
from meshweaver.core.errors import StorageError
from meshweaver.storage.database import Database


class TestBrokerStorage(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self.temp_dir.name) / "broker_test.db"

        self.db = Database(str(db_path))
        self.db.initialize()

        self.broker = Broker(database=self.db)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_submit_task_persists_to_database(self):
        task = self.broker.submit_task("add_numbers", 10, 20)

        stored_task = self.db.get_task(task.task_id)

        self.assertIsNotNone(stored_task)
        self.assertEqual(stored_task["task_type"], "add_numbers")
        self.assertEqual(stored_task["status"], "QUEUED")

        payload = json.loads(stored_task["payload"])
        self.assertEqual(payload["args"], [10, 20])
        self.assertEqual(payload["kwargs"], {})

        self.assertEqual(self.broker.task_count(), 1)
        self.assertEqual(self.broker.pending_count(), 1)

    def test_submit_task_once_does_not_duplicate(self):
        first = self.broker.submit_task_once(
            "request-1", "add_numbers", 10, 20
        )

        second = self.broker.submit_task_once(
            "request-1", "add_numbers", 10, 20
        )

        self.assertEqual(first.task_id, second.task_id)
        self.assertEqual(len(self.db.get_all_tasks()), 1)
        self.assertEqual(self.broker.task_count(), 1)
        self.assertEqual(self.broker.pending_count(), 1)

    def test_storage_failure_does_not_enqueue_task(self):
        failing_database = Mock()
        failing_database.add_task.side_effect = RuntimeError(
            "Simulated database failure"
        )

        broker = Broker(database=failing_database)

        with self.assertRaises(StorageError):
            broker.submit_task("add_numbers", 10, 20)

        self.assertEqual(broker.task_count(), 0)
        self.assertEqual(broker.pending_count(), 0)

    def test_task_lifecycle_persists_all_status_updates(self):
        # Register peer in database
        self.db.add_peer(
            peer_id="peer-1",
            host="127.0.0.1",
            port=5001,
            status="online",
        )

        peer = PeerInfo(
            peer_id="peer-1",
            cpu_usage=20,
            running_tasks=0,
            max_tasks=5,
            is_online=True,
        )

        # Submit task
        task = self.broker.submit_task("add_numbers", 10, 20)

        stored_task = self.db.get_task(task.task_id)
        self.assertEqual(
            stored_task["status"],
            TaskStatus.QUEUED.value,
        )

        # Assign task through Broker
        assignment = self.broker.assign_next_task([peer])

        self.assertIsNotNone(assignment)

        assigned_task, selected_peer = assignment

        self.assertEqual(assigned_task.task_id, task.task_id)
        self.assertEqual(selected_peer.peer_id, "peer-1")

        stored_task = self.db.get_task(task.task_id)

        self.assertEqual(
            stored_task["status"],
            TaskStatus.ASSIGNED.value,
        )
        self.assertEqual(stored_task["assigned_peer_id"], "peer-1")

        # Update task to RUNNING
        self.db.update_task(
            task_id=task.task_id,
            status=TaskStatus.RUNNING.value,
        )

        stored_task = self.db.get_task(task.task_id)

        self.assertEqual(
            stored_task["status"],
            TaskStatus.RUNNING.value,
        )

        # Update task to COMPLETED and save result
        self.db.update_task(
            task_id=task.task_id,
            status=TaskStatus.COMPLETED.value,
            result=json.dumps(30),
        )

        # Verify final persisted state
        stored_task = self.db.get_task(task.task_id)

        self.assertEqual(
            stored_task["status"],
            TaskStatus.COMPLETED.value,
        )
        self.assertEqual(stored_task["assigned_peer_id"], "peer-1")
        self.assertEqual(json.loads(stored_task["result"]), 30)


if __name__ == "__main__":
    unittest.main()
