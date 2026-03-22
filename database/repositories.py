"""Repository pattern implementations for database operations.

Each repository encapsulates all database operations for a specific entity,
following the Single Responsibility Principle (SRP). Repositories depend on
abstractions (connection pool) rather than creating connections directly,
following Dependency Inversion Principle (DIP).
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List, Optional, TypeVar

import psycopg2

from database.connection import ConnectionPool
from database.exceptions import IntegrityError, QueryError, SchemaError
from models import Candidate, Vote, Voter

logger = logging.getLogger(__name__)

T = TypeVar("T")


class BaseRepository(ABC, Generic[T]):
    """Abstract base repository defining common interface."""

    def __init__(self, pool: Optional[ConnectionPool] = None):
        """Initialize repository with connection pool.

        Args:
            pool: Connection pool instance. If None, uses singleton.
        """
        self._pool = pool or ConnectionPool.get_instance()

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Return the database table name for this repository."""
        pass

    @property
    @abstractmethod
    def create_table_sql(self) -> str:
        """Return CREATE TABLE SQL statement."""
        pass

    def create_table(self) -> None:
        """Create the database table if it doesn't exist."""
        try:
            with self._pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(self.create_table_sql)
            logger.info(f"Table '{self.table_name}' created/verified")
        except psycopg2.Error as e:
            raise SchemaError(f"Failed to create table '{self.table_name}': {e}")

    @abstractmethod
    def insert(self, entity: T) -> None:
        """Insert an entity into the database."""
        pass


class CandidateRepository(BaseRepository[Candidate]):
    """Repository for Candidate entity operations."""

    @property
    def table_name(self) -> str:
        return "candidates"

    @property
    def create_table_sql(self) -> str:
        return """
            CREATE TABLE IF NOT EXISTS candidates (
                candidate_id VARCHAR(255) PRIMARY KEY,
                candidate_name VARCHAR(255),
                party_affiliation VARCHAR(255),
                biography TEXT,
                campaign_platform TEXT,
                photo_url TEXT
            )
        """

    @property
    def _insert_sql(self) -> str:
        return """
            INSERT INTO candidates (
                candidate_id, candidate_name, party_affiliation,
                biography, campaign_platform, photo_url
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """

    def insert(self, candidate: Candidate) -> None:
        """Insert a candidate into the database."""
        try:
            with self._pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(self._insert_sql, candidate.to_db_tuple())
            logger.debug(f"Inserted candidate: {candidate.candidate_id}")
        except psycopg2.IntegrityError:
            raise IntegrityError(
                f"Candidate '{candidate.candidate_id}' already exists",
                constraint="candidates_pkey",
            )
        except psycopg2.Error as e:
            raise QueryError(
                f"Failed to insert candidate: {e}",
                query=self._insert_sql,
                original_error=e,
            )

    def get_all(self) -> List[Candidate]:
        """Retrieve all candidates from the database."""
        try:
            with self._pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM candidates")
                    rows = cur.fetchall()

            candidates = []
            for row in rows:
                candidates.append(
                    Candidate(
                        candidate_id=row[0],
                        candidate_name=row[1],
                        party_affiliation=row[2],
                        biography=row[3],
                        campaign_platform=row[4],
                        photo_url=row[5],
                    )
                )
            return candidates

        except psycopg2.Error as e:
            raise QueryError(
                f"Failed to fetch candidates: {e}",
                query="SELECT * FROM candidates",
                original_error=e,
            )

    def get_as_json(self) -> List[Dict[str, Any]]:
        """Retrieve all candidates as JSON-serializable dictionaries.

        Uses PostgreSQL's row_to_json for efficient conversion.
        """
        try:
            with self._pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT row_to_json(t)
                        FROM (SELECT * FROM candidates) t
                    """
                    )
                    rows = cur.fetchall()

            return [row[0] for row in rows]

        except psycopg2.Error as e:
            raise QueryError(
                f"Failed to fetch candidates as JSON: {e}",
                original_error=e,
            )

    def count(self) -> int:
        """Count total candidates."""
        try:
            with self._pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM candidates")
                    return cur.fetchone()[0]
        except psycopg2.Error as e:
            raise QueryError(f"Failed to count candidates: {e}", original_error=e)


class VoterRepository(BaseRepository[Voter]):
    """Repository for Voter entity operations."""

    @property
    def table_name(self) -> str:
        return "voters"

    @property
    def create_table_sql(self) -> str:
        return """
            CREATE TABLE IF NOT EXISTS voters (
                voter_id VARCHAR(255) PRIMARY KEY,
                voter_name VARCHAR(255),
                date_of_birth VARCHAR(255),
                gender VARCHAR(255),
                nationality VARCHAR(255),
                registration_number VARCHAR(255),
                address_street VARCHAR(255),
                address_city VARCHAR(255),
                address_state VARCHAR(255),
                address_country VARCHAR(255),
                address_postcode VARCHAR(255),
                email VARCHAR(255),
                phone_number VARCHAR(255),
                cell_number VARCHAR(255),
                picture TEXT,
                registered_age INTEGER
            )
        """

    @property
    def _insert_sql(self) -> str:
        return """
            INSERT INTO voters (
                voter_id, voter_name, date_of_birth, gender, nationality,
                registration_number, address_street, address_city, address_state,
                address_country, address_postcode, email, phone_number,
                cell_number, picture, registered_age
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

    def insert(self, voter: Voter) -> None:
        """Insert a voter into the database."""
        try:
            with self._pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(self._insert_sql, voter.to_db_tuple())
            logger.debug(f"Inserted voter: {voter.voter_id}")
        except psycopg2.IntegrityError:
            raise IntegrityError(
                f"Voter '{voter.voter_id}' already exists",
                constraint="voters_pkey",
            )
        except psycopg2.Error as e:
            raise QueryError(
                f"Failed to insert voter: {e}",
                query=self._insert_sql,
                original_error=e,
            )

    def count(self) -> int:
        """Count total voters."""
        try:
            with self._pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM voters")
                    return cur.fetchone()[0]
        except psycopg2.Error as e:
            raise QueryError(f"Failed to count voters: {e}", original_error=e)


class VoteRepository(BaseRepository[Vote]):
    """Repository for Vote entity operations."""

    @property
    def table_name(self) -> str:
        return "votes"

    @property
    def create_table_sql(self) -> str:
        return """
            CREATE TABLE IF NOT EXISTS votes (
                voter_id VARCHAR(255) UNIQUE,
                candidate_id VARCHAR(255),
                voting_time TIMESTAMP,
                vote INT DEFAULT 1,
                PRIMARY KEY (voter_id, candidate_id)
            )
        """

    @property
    def _insert_sql(self) -> str:
        return """
            INSERT INTO votes (voter_id, candidate_id, voting_time)
            VALUES (%s, %s, %s)
        """

    def insert(self, vote: Vote) -> None:
        """Insert a vote into the database."""
        try:
            with self._pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(self._insert_sql, vote.to_db_tuple())
            logger.debug(f"Inserted vote: voter={vote.voter_id}")
        except psycopg2.IntegrityError:
            raise IntegrityError(
                f"Voter '{vote.voter_id}' has already voted",
                constraint="votes_voter_id_key",
            )
        except psycopg2.Error as e:
            raise QueryError(
                f"Failed to insert vote: {e}",
                query=self._insert_sql,
                original_error=e,
            )


def create_all_tables() -> None:
    """Convenience function to create all tables."""
    CandidateRepository().create_table()
    VoterRepository().create_table()
    VoteRepository().create_table()
    logger.info("All tables created/verified")
