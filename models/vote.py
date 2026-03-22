"""
Vote data model representing a cast ballot with enriched information.

This model combines core voting data (voter ID, candidate ID, timestamp) with
denormalized voter and candidate information. The denormalization is intentional
for real-time streaming - it allows Spark to aggregate votes without repeatedly
joining to PostgreSQL tables.

Replaces the manual dictionary merging in voting.py:82-85 and the duplicate
schema definition in spark-streaming.py:19-46.
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class Vote(BaseModel):
    """
    Represents a cast vote with enriched voter and candidate data.

    This is a "fat" model that intentionally duplicates voter and candidate
    information alongside the core vote data. This design choice supports
    real-time stream processing by avoiding expensive database joins.

    Data flow:
    1. voting.py: Creates Vote by merging voter + candidate dicts
    2. Kafka: Serializes to JSON and streams to votes_topic
    3. spark-streaming.py: Deserializes using this schema for aggregation
    4. streamlit-app.py: Displays enriched vote data in dashboard

    Alternative considered: Store only IDs and join in Spark
    Rejected because: Requires PostgreSQL lookups during streaming,
    adding latency and database load.
    """

    # Core vote data - required fields
    voter_id: str = Field(..., description="Unique voter identifier (UUID)")
    candidate_id: str = Field(..., description="Unique candidate identifier (UUID)")
    voting_time: str = Field(..., description="Timestamp when vote was cast (YYYY-MM-DD HH:MM:SS)")
    vote: int = Field(default=1, ge=1, description="Vote count (always 1 per voter)")

    # Denormalized voter information - optional because database only stores IDs
    # These fields enable analytics without database joins during streaming
    voter_name: Optional[str] = Field(None, description="Voter's full name")
    date_of_birth: Optional[str] = Field(None, description="Voter's date of birth")
    gender: Optional[str] = Field(None, description="Voter's gender")
    nationality: Optional[str] = Field(None, description="Voter's nationality code")
    registration_number: Optional[str] = Field(None, description="Voter's registration number")
    email: Optional[str] = Field(None, description="Voter's email address")
    phone_number: Optional[str] = Field(None, description="Voter's phone number")
    cell_number: Optional[str] = Field(None, description="Voter's cell number")
    picture: Optional[str] = Field(None, description="Voter's picture URL")
    registered_age: Optional[int] = Field(None, description="Voter's age at registration")

    # Address kept as dict to match Spark StructType schema in spark-streaming.py:33-39
    address: Optional[dict] = Field(None, description="Voter's address (nested dict)")

    # Denormalized candidate information - enables vote aggregation by party/candidate
    candidate_name: Optional[str] = Field(None, description="Candidate's full name")
    party_affiliation: Optional[str] = Field(None, description="Candidate's political party")
    biography: Optional[str] = Field(None, description="Candidate's biography")
    campaign_platform: Optional[str] = Field(None, description="Candidate's campaign platform")
    photo_url: Optional[str] = Field(None, description="Candidate's photo URL")

    class Config:
        """Pydantic model configuration."""
        # Allow modification after creation
        frozen = False
        json_schema_extra = {
            "example": {
                "voter_id": "123e4567-e89b-12d3-a456-426614174000",
                "candidate_id": "123e4567-e89b-12d3-a456-426614174001",
                "voting_time": "2024-02-28 10:30:00",
                "vote": 1,
                "voter_name": "John Doe",
                "candidate_name": "Jane Smith",
                "party_affiliation": "Management Party",
                "photo_url": "https://randomuser.me/api/portraits/women/1.jpg"
            }
        }

    def to_db_tuple(self) -> tuple:
        """
        Converts vote to minimal tuple for PostgreSQL insertion.

        Follows Interface Segregation Principle by returning ONLY the fields
        that the database needs. The votes table (main.py:54-61) only stores
        the relationship between voter and candidate, not all the enriched data.

        The database schema is intentionally minimal because:
        - Full voter/candidate data already exists in their respective tables
        - Storing IDs only ensures referential integrity via foreign keys
        - Enriched data flows through Kafka for real-time processing

        Used by voting.py:90-92 for database insertion.

        Returns:
            Tuple of 3 elements: (voter_id, candidate_id, voting_time)
        """
        return (
            self.voter_id,
            self.candidate_id,
            self.voting_time
        )

    @classmethod
    def from_voter_and_candidate(
        cls,
        voter: dict,
        candidate: dict,
        voting_time: str = None
    ) -> 'Vote':
        """
        Factory method to create an enriched Vote from voter and candidate data.

        Uses dynamic field extraction via Pydantic's model_fields introspection
        to automatically include all relevant fields without hardcoding field names.
        This follows DRY principle - when Vote model fields change, this method
        automatically adapts without code changes.

        The enrichment (copying all voter and candidate fields) happens here to
        prepare data for Kafka streaming. Spark will consume this enriched data
        for real-time aggregation without needing database lookups.

        Implementation:
        1. Start with core vote data (voter_id, candidate_id, voting_time, vote)
        2. Dynamically extract voter fields that exist in Vote model
        3. Dynamically extract candidate fields that exist in Vote model
        4. Pydantic validates the merged data and creates Vote instance

        This replaces 20+ lines of hardcoded field mappings with a 3-line
        dynamic solution that automatically stays in sync with the Vote schema.

        Args:
            voter: Dictionary with voter data (from Voter.model_dump())
            candidate: Dictionary with candidate data (from Candidate.model_dump())
            voting_time: Optional timestamp string. If None, uses current UTC time.

        Returns:
            Vote instance with all fields populated (core + enriched data)

        Example:
            voter = Voter(**voter_data).model_dump()
            candidate = Candidate(**candidate_data).model_dump()
            vote = Vote.from_voter_and_candidate(voter, candidate)
        """
        # Default to current time if not provided
        if voting_time is None:
            voting_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # Start with core vote data
        vote_data = {
            'voter_id': voter.get('voter_id'),
            'candidate_id': candidate.get('candidate_id'),
            'voting_time': voting_time,
            'vote': 1
        }

        # Dynamically extract all voter fields that exist in Vote model
        # Uses Pydantic's model_fields to introspect the Vote schema
        # Only includes fields that are defined in the Vote model (safe merge)
        vote_data.update({
            field_name: voter.get(field_name)
            for field_name in voter.keys()
            if field_name in cls.model_fields and field_name not in vote_data
        })

        # Dynamically extract all candidate fields that exist in Vote model
        # Candidate fields won't overlap with voter fields due to distinct naming
        # (voter_name vs candidate_name, picture vs photo_url, etc.)
        vote_data.update({
            field_name: candidate.get(field_name)
            for field_name in candidate.keys()
            if field_name in cls.model_fields and field_name not in vote_data
        })

        # Pydantic validates all fields and creates the Vote instance
        # Any missing required fields or type mismatches will raise ValidationError
        return cls(**vote_data)