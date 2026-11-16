
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
