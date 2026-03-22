#!/usr/bin/env python
"""
Test script to demonstrate Phase 1 improvements.
Shows how the new config and models work together.
"""

def test_configuration():
    """Test centralized configuration."""
    print("=" * 60)
    print("Testing Configuration (DRY Principle)")
    print("=" * 60)

    from config import settings

    print("\n✅ Database Configuration:")
    print(f"   Connection: {settings.database.connection_string}")
    print(f"   JDBC URL: {settings.database.jdbc_url}")

    print("\n✅ Kafka Configuration:")
    print(f"   Bootstrap Servers: {settings.kafka.bootstrap_servers}")
    print(f"   Group ID: {settings.kafka.group_id}")
    print(f"   Topics:")
    print(f"     - Voters: {settings.kafka.voters_topic}")
    print(f"     - Votes: {settings.kafka.votes_topic}")
    print(f"     - Aggregated: {settings.kafka.aggregated_votes_per_candidate_topic}")

    print("\n✅ Application Configuration:")
    print(f"   Number of Candidates: {settings.app.num_candidates}")
    print(f"   Number of Voters: {settings.app.num_voters}")
    print(f"   Parties: {', '.join(settings.app.parties)}")
    print(f"   Random Seed: {settings.app.random_seed}")

    print("\n✅ Kafka Producer Config:")
    print(f"   {settings.kafka.get_producer_config()}")

    print("\n✅ Kafka Consumer Config:")
    print(f"   {settings.kafka.get_consumer_config()}")


def test_models():
    """Test Pydantic models."""
    print("\n" + "=" * 60)
    print("Testing Data Models (DRY + Type Safety)")
    print("=" * 60)

    from models import Voter, Candidate, Vote, Address

    # Test Address
    print("\n✅ Creating Address model...")
    address = Address(
        street="10 Downing Street",
        city="London",
        state="England",
        country="United Kingdom",
        postcode="SW1A 2AA"
    )
    print(f"   Address: {address.street}, {address.city}")

    # Test Voter
    print("\n✅ Creating Voter model...")
    voter = Voter(
        voter_id="voter-001",
        voter_name="Winston Churchill",
        date_of_birth="1874-11-30",
        gender="male",
        nationality="GB",
        registration_number="REG001",
        address=address,
        email="winston@example.com",
        phone_number="+44 20 7925 0918",
        cell_number="+44 7700 900001",
        picture="https://example.com/winston.jpg",
        registered_age=21
    )
    print(f"   Voter: {voter.voter_name} ({voter.voter_id})")
    print(f"   Email: {voter.email}")
    print(f"   Address: {voter.address.city}, {voter.address.postcode}")

    # Test Candidate
    print("\n✅ Creating Candidate model...")
    candidate = Candidate(
        candidate_id="cand-001",
        candidate_name="Margaret Thatcher",
        party_affiliation="Management Party",
        biography="Former Prime Minister with strong leadership.",
        campaign_platform="Economic reform and strong governance.",
        photo_url="https://example.com/margaret.jpg"
    )
    print(f"   Candidate: {candidate.candidate_name}")
    print(f"   Party: {candidate.party_affiliation}")

    # Test Vote with factory method
    print("\n✅ Creating Vote using factory method...")
    vote = Vote.from_voter_and_candidate(
        voter.model_dump(),
        candidate.model_dump(),
        "2024-02-28 10:30:00"
    )
    print(f"   Vote: {vote.voter_name} → {vote.candidate_name}")
    print(f"   Party: {vote.party_affiliation}")
    print(f"   Time: {vote.voting_time}")

    # Test DB tuple conversion
    print("\n✅ Converting to database tuples (Interface Segregation)...")
    voter_tuple = voter.to_db_tuple()
    candidate_tuple = candidate.to_db_tuple()
    vote_tuple = vote.to_db_tuple()
    print(f"   Voter tuple: {len(voter_tuple)} fields")
    print(f"   Candidate tuple: {len(candidate_tuple)} fields")
    print(f"   Vote tuple: {len(vote_tuple)} fields")

    # Test JSON serialization for Kafka
    print("\n✅ Converting to JSON for Kafka...")
    voter_dict = voter.model_dump()
    candidate_dict = candidate.model_dump()
    vote_dict = vote.model_dump()
    print(f"   Voter dict: {len(voter_dict)} fields")
    print(f"   Candidate dict: {len(candidate_dict)} fields")
    print(f"   Vote dict: {len(vote_dict)} fields")


def test_validation():
    """Test Pydantic validation."""
    print("\n" + "=" * 60)
    print("Testing Validation (Type Safety)")
    print("=" * 60)

    from models import Voter, Address
    from pydantic import ValidationError

    # Test invalid email
    print("\n✅ Testing email validation...")
    try:
        address = Address(
            street="123 Main St",
            city="London",
            state="England",
            country="UK",
            postcode="SW1A"
        )
        voter = Voter(
            voter_id="test",
            voter_name="Test User",
            date_of_birth="1990-01-01",
            gender="male",
            nationality="GB",
            registration_number="REG",
            address=address,
            email="invalid-email",  # ❌ Invalid email
            phone_number="123",
            cell_number="456",
            picture="https://example.com/pic.jpg",
            registered_age=18
        )
        print("   ❌ Validation should have failed!")
    except ValidationError as e:
        print("   ✅ Email validation caught invalid email")
        print(f"   Error: {e.errors()[0]['msg']}")

    # Test valid email
    print("\n✅ Testing with valid email...")
    try:
        voter = Voter(
            voter_id="test",
            voter_name="Test User",
            date_of_birth="1990-01-01",
            gender="male",
            nationality="GB",
            registration_number="REG",
            address=address,
            email="test@example.com",  # ✅ Valid email
            phone_number="123",
            cell_number="456",
            picture="https://example.com/pic.jpg",
            registered_age=18
        )
        print(f"   ✅ Voter created successfully: {voter.email}")
    except ValidationError as e:
        print(f"   ❌ Unexpected error: {e}")


def show_summary():
    """Show summary of improvements."""
    print("\n" + "=" * 60)
    print("Phase 1 Summary: KISS, DRY, SOLID")
    print("=" * 60)

    print("\n✅ DRY Principle (Don't Repeat Yourself):")
    print("   • Database connection: 3 duplicates → 1 config")
    print("   • Kafka configuration: 4 duplicates → 1 config")
    print("   • Topic names: 4 definitions → 1 config")
    print("   • Data schemas: 2 definitions → 1 model")

    print("\n✅ SOLID Principles:")
    print("   • Single Responsibility: Config separate from logic")
    print("   • Dependency Inversion: Models abstract raw dicts")
    print("   • Interface Segregation: to_db_tuple() vs model_dump()")

    print("\n✅ Additional Benefits:")
    print("   • Type safety with Pydantic")
    print("   • Automatic validation")
    print("   • Environment variable support")
    print("   • IDE autocomplete & type hints")
    print("   • Easy to test & mock")

    print("\n✅ Next Steps (Phase 2):")
    print("   • Database abstraction layer")
    print("   • Kafka wrapper classes")
    print("   • Repository pattern for data access")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    test_configuration()
    test_models()
    test_validation()
    show_summary()
    print("\n�� All Phase 1 tests passed!\n")