"""
Candidate data model with automatic validation.

Provides a type-safe structure for election candidate information. This model
is used across the entire application pipeline: data generation (main.py),
vote processing (voting.py), stream aggregation (spark-streaming.py), and
dashboard display (streamlit-app.py).

Eliminates the need for manually constructing candidate dictionaries and
ensures consistent data structure throughout the application.
"""

from pydantic import BaseModel, Field


class Candidate(BaseModel):
    """
    Type-safe candidate model with validation.

    Represents an election candidate with their identifying information,
    party affiliation, and campaign details. Simpler than the Voter model
    as it contains fewer fields.

    Used in:
    - main.py: Initial candidate generation and database insertion
    - voting.py: Random candidate selection for vote assignment
    - spark-streaming.py: Vote aggregation by candidate
    - streamlit-app.py: Dashboard display of candidate information
    """

    # Core identification
    candidate_id: str = Field(..., description="Unique candidate identifier (UUID)")
    candidate_name: str = Field(..., min_length=1, description="Full name of the candidate")

    # Political information
    party_affiliation: str = Field(
        ..., description="Political party (from config.app.parties list)"
    )

    # Campaign information
    biography: str = Field(..., description="Brief biography of the candidate")
    campaign_platform: str = Field(..., description="Key campaign promises or platform")

    # Visual identification
    photo_url: str = Field(..., description="URL to candidate's official photo")

    class Config:
        """Pydantic model configuration."""

        # Allow modification after creation
        frozen = False
        json_schema_extra = {
            "example": {
                "candidate_id": "123e4567-e89b-12d3-a456-426614174001",
                "candidate_name": "Jane Smith",
                "party_affiliation": "Management Party",
                "biography": "A brief bio of the candidate.",
                "campaign_platform": "Key campaign promises or platform.",
                "photo_url": "https://randomuser.me/api/portraits/women/1.jpg",
            }
        }

    def to_db_tuple(self) -> tuple:
        """
        Converts candidate data to a tuple for PostgreSQL insertion.

        Follows Interface Segregation Principle by providing only the data needed
        for database operations, in the exact order expected by the INSERT statement
        in main.py:160-164.

        This is simpler than the Voter model as all candidate fields are already flat
        (no nested address structure to flatten).

        Returns:
            Tuple of 6 elements in database column order
        """
        return (
            self.candidate_id,
            self.candidate_name,
            self.party_affiliation,
            self.biography,
            self.campaign_platform,
            self.photo_url,
        )
