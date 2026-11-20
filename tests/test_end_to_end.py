
import json
import tempfile
import unittest
from pathlib import Path

from meshweaver.broker.broker import Broker
from meshweaver.broker.models import TaskStatus
from meshweaver.broker.peers import PeerInfo
from meshweaver.storage.database import Database
from worker.worker import Worker


class TestEndToEnd(unittest.TestCase):

    def test_submit_assign_execute_and_save_result(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "e2e_test.db"
            db = Database(str(db_path))
            db.initialize()

            # Register an available peer
            db.add_peer(
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

            # Submit task through broker
            broker = Broker(database=db)
            task = broker.submit_task("add_numbers", 10, 20)

            self.assertEqual(task.status, TaskStatus.QUEUED)

            stored_task = db.get_task(task.task_id)
            self.assertEqual(stored_task["status"], TaskStatus.QUEUED.value)

            # Assign task through the actual Broker integration
            assignment = broker.assign_next_task([peer])

            self.assertIsNotNone(assignment)

            assigned_task, selected_peer = assignment

            self.assertEqual(assigned_task.task_id, task.task_id)
            self.assertEqual(assigned_task.status, TaskStatus.ASSIGNED)
            self.assertEqual(selected_peer.peer_id, "peer-1")

            stored_task = db.get_task(task.task_id)
            self.assertEqual(stored_task["status"], TaskStatus.ASSIGNED.value)
            self.assertEqual(stored_task["assigned_peer_id"], "peer-1")

            # Execute task through worker
            worker = Worker()
            worker.register_handler("add_numbers", lambda a, b: a + b)

            task.status = TaskStatus.RUNNING
            db.update_task(
                task_id=task.task_id,
                status=task.status.value,
                assigned_peer_id=selected_peer.peer_id,
            )

            stored_task = db.get_task(task.task_id)
            self.assertEqual(stored_task["status"], TaskStatus.RUNNING.value)

            result = worker.execute(task.name, *task.args)

            # Save completed status and result
            task.status = TaskStatus.COMPLETED
            db.update_task(
                task_id=task.task_id,
                status=task.status.value,
                assigned_peer_id=selected_peer.peer_id,
                result=json.dumps(result),
            )

            # Verify final persisted result
            stored_task = db.get_task(task.task_id)

            self.assertEqual(stored_task["status"], TaskStatus.COMPLETED.value)
            self.assertEqual(stored_task["assigned_peer_id"], "peer-1")
            self.assertEqual(json.loads(stored_task["result"]), 30)


if __name__ == "__main__":
    unittest.main()
