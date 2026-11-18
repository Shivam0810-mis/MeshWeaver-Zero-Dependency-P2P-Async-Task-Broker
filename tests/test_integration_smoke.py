
import json
import tempfile
import unittest
from pathlib import Path

from meshweaver.broker.broker import Broker
from meshweaver.broker.models import TaskStatus
from meshweaver.config.loader import load_config
from meshweaver.storage.database import Database


class TestIntegrationSmoke(unittest.TestCase):

    def test_basic_system_integration(self):
        # 1. Load application configuration
        config = load_config()

        self.assertIsInstance(config, dict)
        self.assertIn("node_id", config)

        # 2. Create broker and submit a task
        broker = Broker()

        task = broker.submit_task(
            "add_numbers",
            10,
            20,
        )

        self.assertEqual(task.name, "add_numbers")
        self.assertEqual(task.args, (10, 20))
        self.assertEqual(task.status, TaskStatus.QUEUED)
        self.assertEqual(broker.task_count(), 1)

        # 3. Verify task retrieval from broker
        fetched_task = broker.get_task(task.task_id)

        self.assertEqual(fetched_task.task_id, task.task_id)

        # 4. Create a temporary SQLite database
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "smoke_test.db"

            db = Database(str(database_path))
            db.initialize()

            # 5. Save and retrieve a peer
            db.add_peer(
                peer_id="smoke-peer-1",
                host="127.0.0.1",
                port=5001,
                status="online",
            )

            peer = db.get_peer("smoke-peer-1")

            self.assertIsNotNone(peer)
            self.assertEqual(peer["host"], "127.0.0.1")
            self.assertEqual(peer["port"], 5001)

            # 6. Convert broker task into a JSON payload
            payload = json.dumps({
                "args": task.args,
                "kwargs": task.kwargs,
            })

            # 7. Save broker task in SQLite
            db.add_task(
                task_id=task.task_id,
                task_type=task.name,
                payload=payload,
                status=task.status.value,
            )

            # 8. Retrieve and verify persisted task
            stored_task = db.get_task(task.task_id)

            self.assertIsNotNone(stored_task)
            self.assertEqual(stored_task["task_type"], "add_numbers")
            self.assertEqual(stored_task["status"], "QUEUED")

            stored_payload = json.loads(stored_task["payload"])
            self.assertEqual(stored_payload["args"], [10, 20])

        print("\nIntegration smoke test passed!")


if __name__ == "__main__":
    unittest.main()
