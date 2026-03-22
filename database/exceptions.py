"""Custom exceptions for database operations."""

from typing import Optional


class DatabaseError(Exception):
    """Base exception for all database-related errors."""

    pass


class ConnectionError(DatabaseError):
    """Raised when database connection fails."""

    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message)
        self.original_error = original_error


class ConnectionPoolExhaustedError(DatabaseError):
    """Raised when no connections are available in the pool."""

    pass


class QueryError(DatabaseError):
    """Raised when a query execution fails."""

    def __init__(
        self,
        message: str,
        query: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.query = query
        self.original_error = original_error


class IntegrityError(DatabaseError):
    """Raised when a database constraint is violated (e.g., duplicate key)."""

    def __init__(self, message: str, constraint: Optional[str] = None):
        super().__init__(message)
        self.constraint = constraint


class SchemaError(DatabaseError):
    """Raised when schema operations fail (CREATE TABLE, ALTER, etc.)."""

    pass
