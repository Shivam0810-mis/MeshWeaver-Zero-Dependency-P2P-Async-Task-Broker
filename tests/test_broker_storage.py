
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from meshweaver.broker.broker import Broker
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


if __name__ == "__main__":
    unittest.main()
