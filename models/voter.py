"""
Voter data model with automatic validation.

Provides a type-safe, validated structure for voter information used throughout
the application. This eliminates the need to manually construct voter dictionaries
and duplicate field definitions across multiple files.

Previously, voter data was handled as raw dictionaries with no validation,
leading to potential runtime errors from typos, missing fields, or invalid data.
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, EmailStr


class Address(BaseModel):
    """
    Represents a voter's physical address.

    Nested within the Voter model to maintain the same structure used by
    the external randomuser.me API and expected by Spark streaming jobs.
    """
    street: str  # Full street address including number
    city: str    # City name
    state: str   # State/province/region
    country: str # Country name
    postcode: str # Postal/ZIP code

    class Config:
        """Pydantic model configuration."""
        # Allow modification after creation (needed for data transformations)
        frozen = False


class Voter(BaseModel):
    """
    Type-safe voter model with automatic validation.

    Replaces raw dictionary usage in main.py, voting.py, and spark-streaming.py.
    Provides compile-time type checking, runtime validation, and IDE autocomplete.

    Key benefits over raw dictionaries:
    - Automatic email validation (catches invalid emails before database insertion)
    - Required field enforcement (prevents missing data)
    - Type safety (prevents assigning wrong data types)
    - Self-documenting structure (clear field purposes)
    """

    # Core identification fields
    voter_id: str = Field(..., description="Unique voter identifier (UUID)")
    voter_name: str = Field(..., min_length=1, description="Full name of the voter")

    # Demographics
    date_of_birth: str = Field(..., description="Date of birth in ISO format")
    gender: str = Field(..., description="Gender of the voter")
    nationality: str = Field(..., max_length=2, description="ISO country code (e.g., 'GB')")
    registered_age: int = Field(..., ge=0, le=150, description="Age when voter registered")

    # Registration information
    registration_number: str = Field(..., description="Unique voter registration number")

    # Contact information - email field uses EmailStr for automatic validation
    address: Address = Field(..., description="Voter's physical address")
    email: EmailStr = Field(..., description="Email address (automatically validated)")
    phone_number: str = Field(..., description="Primary phone number")
    cell_number: str = Field(..., description="Cell/mobile phone number")

    # Visual identification
    picture: str = Field(..., description="URL to voter's profile picture")

    class Config:
        """Pydantic model configuration."""
        # Allow modification after creation
        frozen = False
        json_schema_extra = {
            "example": {
                "voter_id": "123e4567-e89b-12d3-a456-426614174000",
                "voter_name": "John Doe",
                "date_of_birth": "1990-01-01T00:00:00.000Z",
                "gender": "male",
                "nationality": "GB",
                "registration_number": "REG123456",
                "address": {
                    "street": "123 Main St",
                    "city": "London",
                    "state": "England",
                    "country": "United Kingdom",
                    "postcode": "SW1A 1AA"
                },
                "email": "john.doe@example.com",
                "phone_number": "+44 20 1234 5678",
                "cell_number": "+44 7700 900000",
                "picture": "https://randomuser.me/api/portraits/men/1.jpg",
                "registered_age": 18
            }
        }

    def to_db_tuple(self) -> tuple:
        """
        Converts voter data to a tuple for PostgreSQL insertion.

        Follows Interface Segregation Principle by providing only the data needed
        for database operations, in the exact order expected by the INSERT statement
        in main.py:120-127.

        The address object is flattened into individual fields (street, city, state, etc.)
        to match the relational database schema.

        Returns:
            Tuple of 16 elements in database column order
        """
        return (
            self.voter_id,
            self.voter_name,
            self.date_of_birth,
            self.gender,
            self.nationality,
            self.registration_number,
            # Flatten address object into individual fields
            self.address.street,
            self.address.city,
            self.address.state,
            self.address.country,
            self.address.postcode,
            # Contact information
            self.email,
            self.phone_number,
            self.cell_number,
            # Additional fields
            self.picture,
            self.registered_age
        )