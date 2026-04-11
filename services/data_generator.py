"""Data generation service using the randomuser.me API.

Responsible solely for fetching and constructing Voter and Candidate
objects from the external API. Extracted from main.py to follow SRP —
data generation is now separate from database and Kafka concerns.
"""

import logging
import time

import requests
from requests.exceptions import RequestException  # used in _get

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

    _MAX_RETRIES = 3
    _RETRY_DELAY = 2  # seconds

    def __init__(self, api_url: str = None):
        self._api_url = api_url or settings.app.randomuser_api_url
        self._parties = settings.app.parties

    def _get(self, url: str) -> dict:
        """GET the URL with retries on timeout or connection errors."""
        for attempt in range(1, self._MAX_RETRIES + 1):
            try:
                response = requests.get(url, timeout=10)
                if response.status_code != 200:
                    raise RuntimeError(f"HTTP {response.status_code}")
                return response.json()
            except RequestException as e:
                if attempt == self._MAX_RETRIES:
                    raise RuntimeError(
                        f"Failed to reach randomuser API after {self._MAX_RETRIES} attempts: {e}"
                    ) from e
                logger.warning(
                    "randomuser API error (attempt %d/%d): %s — retrying in %ds",
                    attempt,
                    self._MAX_RETRIES,
                    e,
                    self._RETRY_DELAY,
                )
                time.sleep(self._RETRY_DELAY)

    def generate_voter(self) -> Voter:
        """Fetch one random user from the API and return a Voter model.

        Returns:
            Validated Voter instance

        Raises:
            RuntimeError: If the API call fails
        """
        user = self._get(self._api_url)["results"][0]
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
        user = self._get(f"{self._api_url}&gender={gender}")["results"][0]
        return Candidate(
            candidate_id=user["login"]["uuid"],
            candidate_name=f"{user['name']['first']} {user['name']['last']}",
            party_affiliation=self._parties[candidate_number % len(self._parties)],
            biography="A brief bio of the candidate.",
            campaign_platform="Key campaign promises or platform.",
            photo_url=user["picture"]["large"],
        )
