
import tempfile
import unittest
from pathlib import Path

from meshweaver.storage.database import Database


class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self.temp_dir.name) / "test_meshweaver.db"

        self.db = Database(str(db_path))
        self.db.initialize()

    def tearDown(self):
        self.temp_dir.cleanup()

    # Peer tests

    def test_add_and_get_peer(self):
        self.db.add_peer("peer-1", "127.0.0.1", 5000)

        peer = self.db.get_peer("peer-1")

        self.assertIsNotNone(peer)
        self.assertEqual(peer["host"], "127.0.0.1")
        self.assertEqual(peer["port"], 5000)

    def test_get_all_peers(self):
        self.db.add_peer("peer-1", "127.0.0.1", 5000)
        self.db.add_peer("peer-2", "127.0.0.2", 5001)

        peers = self.db.get_all_peers()

        self.assertEqual(len(peers), 2)

    def test_update_peer(self):
        self.db.add_peer("peer-1", "127.0.0.1", 5000)

        updated = self.db.update_peer(
            "peer-1", "127.0.0.5", 6000, "active"
        )

        peer = self.db.get_peer("peer-1")

        self.assertTrue(updated)
        self.assertEqual(peer["host"], "127.0.0.5")
        self.assertEqual(peer["status"], "active")

    def test_delete_peer(self):
        self.db.add_peer("peer-1", "127.0.0.1", 5000)

        deleted = self.db.delete_peer("peer-1")

        self.assertTrue(deleted)
        self.assertIsNone(self.db.get_peer("peer-1"))

    # Task tests

    def test_add_and_get_task(self):
        self.db.add_task("task-1", "calculation", '{"value": 10}')

        task = self.db.get_task("task-1")

        self.assertIsNotNone(task)
        self.assertEqual(task["task_type"], "calculation")
        self.assertEqual(task["status"], "pending")

    def test_get_all_tasks(self):
        self.db.add_task("task-1", "calculation", '{"value": 10}')
        self.db.add_task("task-2", "processing", '{"value": 20}')

        tasks = self.db.get_all_tasks()

        self.assertEqual(len(tasks), 2)

    def test_update_task(self):
        self.db.add_task("task-1", "calculation", '{"value": 10}')

        updated = self.db.update_task(
            "task-1", "completed", result='{"answer": 20}'
        )

        task = self.db.get_task("task-1")

        self.assertTrue(updated)
        self.assertEqual(task["status"], "completed")
        self.assertEqual(task["result"], '{"answer": 20}')

    def test_delete_task(self):
        self.db.add_task("task-1", "calculation", '{"value": 10}')

        deleted = self.db.delete_task("task-1")

        self.assertTrue(deleted)
        self.assertIsNone(self.db.get_task("task-1"))


if __name__ == "__main__":
    unittest.main()
