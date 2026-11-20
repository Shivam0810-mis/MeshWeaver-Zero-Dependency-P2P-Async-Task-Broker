import sqlite3
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

    def test_update_task_preserves_optional_fields_when_omitted(self):
        self.db.add_peer("peer-1", "127.0.0.1", 5000)
        self.db.add_task("task-1", "calculation", '{"value": 10}')

        self.db.update_task(
            "task-1",
            "assigned",
            assigned_peer_id="peer-1",
            result='{"answer": 20}',
        )

        updated = self.db.update_task("task-1", "completed")

        task = self.db.get_task("task-1")

        self.assertTrue(updated)
        self.assertEqual(task["status"], "completed")
        self.assertEqual(task["assigned_peer_id"], "peer-1")
        self.assertEqual(task["result"], '{"answer": 20}')
    
    def test_delete_task(self):
        self.db.add_task("task-1", "calculation", '{"value": 10}')

        deleted = self.db.delete_task("task-1")

        self.assertTrue(deleted)
        self.assertIsNone(self.db.get_task("task-1"))


    # Migration tests

    def test_initial_migration_version(self):
        connection = self.db.get_connection()
        try:
            versions = connection.execute(
                "SELECT version FROM schema_migrations"
            ).fetchall()

            self.assertEqual([row["version"] for row in versions], [1])
        finally:
            connection.close()

    def test_initialize_does_not_duplicate_migration(self):
        self.db.initialize()
        self.db.initialize()

        connection = self.db.get_connection()
        try:
            count = connection.execute(
                "SELECT COUNT(*) FROM schema_migrations"
            ).fetchone()[0]

            self.assertEqual(count, 1)
        finally:
            connection.close()

    def test_initialize_preserves_existing_data(self):
        self.db.add_peer("peer-migration", "127.0.0.1", 5000)
        self.db.add_task("task-migration", "calculation", '{"value": 10}')

        self.db.initialize()

        self.assertIsNotNone(self.db.get_peer("peer-migration"))
        self.assertIsNotNone(self.db.get_task("task-migration"))
    
    def test_initialize_applies_new_migration(self):
        class DatabaseWithV2(Database):
            def _get_migrations(self):
                migrations = super()._get_migrations()
                migrations[2] = [
                    """
                    CREATE TABLE migration_test (
                        id INTEGER PRIMARY KEY
                    )
                    """
                ]
                return migrations

        upgraded_db = DatabaseWithV2(self.db.database_path)
        upgraded_db.initialize()

        connection = upgraded_db.get_connection()
        try:
            table = connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name = 'migration_test'
                """
            ).fetchone()

            version = connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0]

            self.assertIsNotNone(table)
            self.assertEqual(version, 2)
        finally:
            connection.close()

    def test_initialize_rolls_back_failed_migration(self):
        class DatabaseWithFailedV2(Database):
            def _get_migrations(self):
                migrations = super()._get_migrations()
                migrations[2] = [
                    "CREATE TABLE rollback_test (id INTEGER PRIMARY KEY)",
                    "INVALID SQL STATEMENT"
                ]
                return migrations

        failing_db = DatabaseWithFailedV2(self.db.database_path)

        with self.assertRaises(sqlite3.OperationalError):
            failing_db.initialize()

        connection = self.db.get_connection()
        try:
            table = connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name = 'rollback_test'
                """
            ).fetchone()

            version = connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0]

            self.assertIsNone(table)
            self.assertEqual(version, 1)
        finally:
            connection.close()
            
    
if __name__ == "__main__":
    unittest.main()