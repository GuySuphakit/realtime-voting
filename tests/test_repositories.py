"""Tests for database repository utilities."""

from unittest.mock import MagicMock, patch

import psycopg2
import pytest

from database.exceptions import SchemaError
from database.repositories import reset_all_tables


class TestResetAllTables:
    def test_executes_truncate_on_success(self):
        """reset_all_tables runs the TRUNCATE statement via the connection pool."""
        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cur
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool = MagicMock()
        mock_pool.get_connection.return_value.__enter__ = lambda s: mock_conn
        mock_pool.get_connection.return_value.__exit__ = MagicMock(return_value=False)

        with patch("database.repositories.ConnectionPool.get_instance", return_value=mock_pool):
            reset_all_tables()

        mock_cur.execute.assert_called_once_with(
            "TRUNCATE votes, voters, candidates RESTART IDENTITY CASCADE"
        )

    def test_raises_schema_error_on_db_failure(self):
        """reset_all_tables wraps psycopg2 errors in SchemaError."""
        mock_pool = MagicMock()
        mock_pool.get_connection.side_effect = psycopg2.OperationalError("connection refused")

        with patch("database.repositories.ConnectionPool.get_instance", return_value=mock_pool):
            with pytest.raises(SchemaError, match="Failed to reset tables"):
                reset_all_tables()
