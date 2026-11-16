
import sqlite3


class Database:
    """Manage SQLite database connections and initialization."""

    def __init__(self, database_path: str = "meshweaver.db"):
        self.database_path = database_path

    def get_connection(self) -> sqlite3.Connection:
        """Create and return a SQLite database connection."""
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def initialize(self) -> None:
        """Create the initial database schema and migration record."""
        connection = self.get_connection()

        try:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS peers (
                    peer_id TEXT PRIMARY KEY,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'unknown',
                    last_seen TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    task_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    assigned_peer_id TEXT,
                    result TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (assigned_peer_id)
                        REFERENCES peers(peer_id)
                )
            """)

            connection.execute("""
                INSERT OR IGNORE INTO schema_migrations (version)
                VALUES (1)
            """)

            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    # Peer CRUD operations

    def add_peer(
        self,
        peer_id: str,
        host: str,
        port: int,
        status: str = "unknown",
        last_seen: str = None,
    ) -> None:
        """Add a new peer to the database."""
        connection = self.get_connection()

        try:
            connection.execute(
                """
                INSERT INTO peers (peer_id, host, port, status, last_seen)
                VALUES (?, ?, ?, ?, ?)
                """,
                (peer_id, host, port, status, last_seen),
            )
            connection.commit()
        finally:
            connection.close()

    def get_peer(self, peer_id: str):
        """Retrieve a peer using its ID."""
        connection = self.get_connection()

        try:
            peer = connection.execute(
                "SELECT * FROM peers WHERE peer_id = ?",
                (peer_id,),
            ).fetchone()

            return dict(peer) if peer else None
        finally:
            connection.close()

    def get_all_peers(self):
        """Retrieve all registered peers."""
        connection = self.get_connection()

        try:
            peers = connection.execute(
                "SELECT * FROM peers ORDER BY created_at"
            ).fetchall()

            return [dict(peer) for peer in peers]
        finally:
            connection.close()

    def update_peer(
        self,
        peer_id: str,
        host: str,
        port: int,
        status: str,
        last_seen: str = None,
    ) -> bool:
        """Update an existing peer."""
        connection = self.get_connection()

        try:
            cursor = connection.execute(
                """
                UPDATE peers
                SET host = ?, port = ?, status = ?, last_seen = ?
                WHERE peer_id = ?
                """,
                (host, port, status, last_seen, peer_id),
            )
            connection.commit()

            return cursor.rowcount > 0
        finally:
            connection.close()

    def delete_peer(self, peer_id: str) -> bool:
        """Delete a peer using its ID."""
        connection = self.get_connection()

        try:
            cursor = connection.execute(
                "DELETE FROM peers WHERE peer_id = ?",
                (peer_id,),
            )
            connection.commit()

            return cursor.rowcount > 0
        finally:
            connection.close()



    # Task CRUD operations

    def add_task(
        self,
        task_id: str,
        task_type: str,
        payload: str,
        status: str = "pending",
        assigned_peer_id: str = None,
        result: str = None,
    ) -> None:
        """Add a new task to the database."""
        connection = self.get_connection()

        try:
            connection.execute(
                """
                INSERT INTO tasks (
                    task_id, task_type, payload, status,
                    assigned_peer_id, result
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    task_type,
                    payload,
                    status,
                    assigned_peer_id,
                    result,
                ),
            )
            connection.commit()
        finally:
            connection.close()

    def get_task(self, task_id: str):
        """Retrieve a task using its ID."""
        connection = self.get_connection()

        try:
            task = connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()

            return dict(task) if task else None
        finally:
            connection.close()

    def get_all_tasks(self):
        """Retrieve all registered tasks."""
        connection = self.get_connection()

        try:
            tasks = connection.execute(
                "SELECT * FROM tasks ORDER BY created_at"
            ).fetchall()

            return [dict(task) for task in tasks]
        finally:
            connection.close()

    def update_task(
        self,
        task_id: str,
        status: str,
        assigned_peer_id: str = None,
        result: str = None,
    ) -> bool:
        """Update an existing task."""
        connection = self.get_connection()

        try:
            cursor = connection.execute(
                """
                UPDATE tasks
                SET status = ?,
                    assigned_peer_id = ?,
                    result = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE task_id = ?
                """,
                (status, assigned_peer_id, result, task_id),
            )
            connection.commit()

            return cursor.rowcount > 0
        finally:
            connection.close()

    def delete_task(self, task_id: str) -> bool:
        """Delete a task using its ID."""
        connection = self.get_connection()

        try:
            cursor = connection.execute(
                "DELETE FROM tasks WHERE task_id = ?",
                (task_id,),
            )
            connection.commit()

            return cursor.rowcount > 0
        finally:
            connection.close()
