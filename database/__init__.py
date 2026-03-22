"""Database abstraction layer for the realtime-voting application.

Provides:
- ConnectionPool: Thread-safe singleton connection pool
- Repositories: Entity-specific database operations
- Exceptions: Typed error handling

Usage:
    from database import CandidateRepository, VoterRepository, create_all_tables

    # Create tables
    create_all_tables()

    # Use repositories
    repo = CandidateRepository()
    repo.insert(candidate)
"""

from database.connection import ConnectionPool, get_connection
from database.exceptions import (
    ConnectionError,
    ConnectionPoolExhaustedError,
    DatabaseError,
    IntegrityError,
    QueryError,
    SchemaError,
)
from database.repositories import (
    CandidateRepository,
    VoteRepository,
    VoterRepository,
    create_all_tables,
)

__all__ = [
    # Connection management
    "ConnectionPool",
    "get_connection",
    # Repositories
    "CandidateRepository",
    "VoterRepository",
    "VoteRepository",
    "create_all_tables",
    # Exceptions
    "DatabaseError",
    "ConnectionError",
    "ConnectionPoolExhaustedError",
    "QueryError",
    "IntegrityError",
    "SchemaError",
]
