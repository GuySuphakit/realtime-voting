"""Tests for Kafka JSON serialization with Pydantic models."""

import json
from datetime import datetime, timezone

from kafka_utils.serializers import serialize_to_json
from models import Vote


def _make_vote() -> Vote:
    return Vote(
        voter_id="voter-abc",
        candidate_id="cand-xyz",
        voting_time=datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc),
        voter_name="Ada Lovelace",
        voter_dob="1815-12-10",
        voter_gender="female",
        voter_nationality="GB",
        voter_registration_number="REG001",
        voter_address={
            "street": "1 Logic Lane",
            "city": "London",
            "state": "England",
            "country": "United Kingdom",
            "postcode": "EC1A 1BB",
        },
        voter_email="ada@example.com",
        voter_phone="01234 567890",
        voter_cell="07700 900001",
        voter_picture="https://example.com/ada.jpg",
        candidate_name="Charles Babbage",
        party_affiliation="Analytical Party",
        biography="Pioneer of computing.",
        campaign_platform="Analytical governance.",
        photo_url="https://example.com/charles.jpg",
    )


class TestSerializeToJson:
    def test_vote_with_datetime_serializes_without_error(self):
        """mode='json' ensures datetime is serialized as ISO string, not object."""
        vote = _make_vote()
        result = serialize_to_json(vote)
        assert isinstance(result, bytes)

    def test_produces_valid_utf8_json(self):
        """Output decodes to valid JSON."""
        vote = _make_vote()
        raw = serialize_to_json(vote)
        parsed = json.loads(raw.decode("utf-8"))
        assert parsed["voter_id"] == "voter-abc"
        assert parsed["candidate_id"] == "cand-xyz"

    def test_datetime_field_is_string_in_output(self):
        """voting_time must be a JSON string, not a Python datetime object."""
        vote = _make_vote()
        raw = serialize_to_json(vote)
        parsed = json.loads(raw.decode("utf-8"))
        assert isinstance(parsed["voting_time"], str)
        assert "2024-03-01" in parsed["voting_time"]

    def test_plain_dict_serializes(self):
        """Plain dict (non-Pydantic) round-trips correctly."""
        data = {"key": "value", "count": 42}
        raw = serialize_to_json(data)
        assert json.loads(raw.decode("utf-8")) == data
