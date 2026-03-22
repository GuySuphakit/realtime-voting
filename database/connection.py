"""Database connection pool manager.

Provides a thread-safe singleton connection pool for PostgreSQL using psycopg2.
Supports context manager protocol for automatic connection management.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from queue import Empty, Queue
from typing import TYPE_CHECKING, Generator, Optional

import psycopg2

from config import settings
from database.exceptions import (
    ConnectionError as DBConnectionError,
    ConnectionPoolExhaustedError,
)

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PgConnection


class ConnectionPool:
    """Thread-safe singleton connection pool for PostgreSQL.

    Attributes:
        min_connections: Minimum connections to maintain (default: 2)
        max_connections: Maximum connections allowed (default: 10)
        connection_timeout: Seconds to wait for available connection (default: 30)

    Usage:
        pool = ConnectionPool.get_instance()

        with pool.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM voters")
                results = cur.fetchall()
    """

    _instance: Optional[ConnectionPool] = None
    _lock: threading.Lock = threading.Lock()

    def __init__(
        self,
        min_connections: int = 2,
        max_connections: int = 10,
        connection_timeout: float = 30.0,
    ):
        """Initialize the connection pool.

        Note: Use get_instance() instead of direct instantiation.
        """
        self._min_connections = min_connections
        self._max_connections = max_connections
        self._connection_timeout = connection_timeout
        self._pool: Queue[PgConnection] = Queue(maxsize=max_connections)
        self._connection_count = 0
        self._pool_lock = threading.Lock()

        self._initialize_pool()

    @classmethod
    def get_instance(
        cls,
        min_connections: int = 2,
        max_connections: int = 10,
        connection_timeout: float = 30.0,
    ) -> ConnectionPool:
        """Get or create the singleton connection pool instance.

        Thread-safe singleton implementation using double-checked locking.
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(
                        min_connections=min_connections,
                        max_connections=max_connections,
                        connection_timeout=connection_timeout,
                    )
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (primarily for testing)."""
        with cls._lock:
            if cls._instance is not None:
                cls._instance.close_all()
                cls._instance = None

    def _initialize_pool(self) -> None:
        """Pre-populate pool with minimum number of connections."""
        for _ in range(self._min_connections):
            try:
                conn = self._create_connection()
                self._pool.put(conn)
            except DBConnectionError:
                # Don't fail initialization if DB is not available yet
                pass

    def _create_connection(self) -> PgConnection:
        """Create a new database connection using settings."""
        try:
            conn = psycopg2.connect(settings.database.connection_string)
            with self._pool_lock:
                self._connection_count += 1
            return conn
        except psycopg2.Error as e:
            raise DBConnectionError(
                f"Failed to connect to database: {e}",
                original_error=e,
            )

    def _validate_connection(self, conn: PgConnection) -> bool:
        """Check if a connection is still valid."""
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except psycopg2.Error:
            return False

    @contextmanager
    def get_connection(self) -> Generator[PgConnection, None, None]:
        """Get a connection from the pool using context manager.

        Automatically handles:
        - Connection acquisition from pool
        - Creating new connection if pool empty and under max
        - Commit on successful exit
        - Rollback on exception
        - Return connection to pool

        Yields:
            PostgreSQL connection

        Raises:
            ConnectionPoolExhaustedError: If no connections available
            DBConnectionError: If connection creation fails
        """
        conn: Optional[PgConnection] = None

        try:
            # Try to get connection from pool
            try:
                conn = self._pool.get(timeout=self._connection_timeout)

                # Validate connection, create new if invalid
                if not self._validate_connection(conn):
                    try:
                        conn.close()
                    except Exception:
                        pass
                    with self._pool_lock:
                        self._connection_count -= 1
                    conn = self._create_connection()

            except Empty:
                # Pool empty, try to create new connection if under max
                with self._pool_lock:
                    if self._connection_count < self._max_connections:
                        conn = self._create_connection()
                    else:
                        raise ConnectionPoolExhaustedError(
                            f"Connection pool exhausted (max={self._max_connections})"
                        )

            yield conn
            conn.commit()

        except Exception:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise

        finally:
            if conn is not None:
                self._pool.put(conn)

    def close_all(self) -> None:
        """Close all connections in the pool."""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
                with self._pool_lock:
                    self._connection_count -= 1
            except Empty:
                break
            except Exception:
                pass

    @property
    def available_connections(self) -> int:
        """Return number of connections currently available in pool."""
        return self._pool.qsize()

    @property
    def total_connections(self) -> int:
        """Return total number of connections created."""
        with self._pool_lock:
            return self._connection_count


@contextmanager
def get_connection() -> Generator[PgConnection, None, None]:
    """Convenience function to get connection from default pool.

    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    """
    with ConnectionPool.get_instance().get_connection() as conn:
        yield conn
