"""Data generation service using the randomuser.me API.

Responsible solely for fetching and constructing Voter and Candidate
objects from the external API. Extracted from main.py to follow SRP —
data generation is now separate from database and Kafka concerns.
"""

import logging

import requests
from requests.exceptions import RequestException

from config import settings
from models import Candidate, Voter
from models.voter import Address

logger = logging.getLogger(__name__)


class DataGeneratorService:
    """Generates realistic voter and candidate data from randomuser.me API.

    Usage:
        generator = DataGeneratorService()
        voter = generator.generate_voter()
        candidate = generator.generate_candidate(candidate_number=0)
    """

    def __init__(self, api_url: str = None):
        self._api_url = api_url or settings.app.randomuser_api_url
        self._parties = settings.app.parties

    def generate_voter(self) -> Voter:
        """Fetch one random user from the API and return a Voter model.

        Returns:
            Validated Voter instance

        Raises:
            RuntimeError: If the API call fails
        """
        try:
            response = requests.get(self._api_url, timeout=10)
        except RequestException as e:
            raise RuntimeError(f"Failed to reach randomuser API: {e}") from e
        if response.status_code != 200:
            raise RuntimeError(f"Failed to fetch voter data: HTTP {response.status_code}")

        user = response.json()["results"][0]
        return Voter(
            voter_id=user["login"]["uuid"],
            voter_name=f"{user['name']['first']} {user['name']['last']}",
            date_of_birth=user["dob"]["date"],
            gender=user["gender"],
            nationality=user["nat"],
            registration_number=user["login"]["username"],
            address=Address(
                street=f"{user['location']['street']['number']} {user['location']['street']['name']}",
                city=user["location"]["city"],
                state=user["location"]["state"],
                country=user["location"]["country"],
                postcode=str(user["location"]["postcode"]),
            ),
            email=user["email"],
            phone_number=user["phone"],
            cell_number=user["cell"],
            picture=user["picture"]["large"],
            registered_age=user["registered"]["age"],
        )

    def generate_candidate(self, candidate_number: int) -> Candidate:
        """Fetch one random user from the API and return a Candidate model.

        Gender alternates by candidate_number to ensure diversity.

        Args:
            candidate_number: Index used to assign party and alternate gender

        Returns:
            Validated Candidate instance

        Raises:
            RuntimeError: If the API call fails
        """
        gender = "female" if candidate_number % 2 == 1 else "male"
        try:
            response = requests.get(f"{self._api_url}&gender={gender}", timeout=10)
        except RequestException as e:
            raise RuntimeError(f"Failed to reach randomuser API: {e}") from e
        if response.status_code != 200:
            raise RuntimeError(f"Failed to fetch candidate data: HTTP {response.status_code}")

        user = response.json()["results"][0]
        return Candidate(
            candidate_id=user["login"]["uuid"],
            candidate_name=f"{user['name']['first']} {user['name']['last']}",
            party_affiliation=self._parties[candidate_number % len(self._parties)],
            biography="A brief bio of the candidate.",
            campaign_platform="Key campaign promises or platform.",
            photo_url=user["picture"]["large"],
        )
