"""Voting service: consumes voter messages and produces vote records.

Responsible solely for the vote processing loop: reading voters from Kafka,
randomly assigning a candidate, persisting the vote, and publishing it back
to Kafka. Extracted from voting.py to follow SRP.
"""

import logging
import random
import time

from config import settings
from database.exceptions import IntegrityError
from database.repositories import VoteRepository
from kafka_utils.consumer import KafkaConsumerWrapper
from kafka_utils.producer import KafkaProducerWrapper
from models import Candidate, Vote

logger = logging.getLogger(__name__)


class VotingService:
    """Processes votes by consuming voters from Kafka and publishing vote records.

    Usage:
        service = VotingService(candidates, vote_repo, consumer, producer)
        service.run()
    """

    def __init__(
        self,
        candidates: list[Candidate],
        vote_repo: VoteRepository,
        consumer: KafkaConsumerWrapper,
        producer: KafkaProducerWrapper,
        voting_delay: float = None,
    ):
        if not candidates:
            raise ValueError("Candidate list must not be empty")

        self._candidates = candidates
        self._vote_repo = vote_repo
        self._consumer = consumer
        self._producer = producer
        self._delay = (
            voting_delay if voting_delay is not None else settings.app.voting_delay_seconds
        )

    def run(self) -> None:
        """Start the voting loop. Runs until the consumer is stopped or interrupted."""
        logger.info("Starting voting service...")
        try:
            for voter_data in self._consumer.consume():
                self._process_vote(voter_data)
                time.sleep(self._delay)
        except KeyboardInterrupt:
            logger.info("Voting service stopped.")

    def _process_vote(self, voter_data: dict) -> None:
        """Assign a random candidate to a voter, persist and publish the vote."""
        candidate = random.choice(self._candidates)
        vote = Vote.from_voter_and_candidate(
            voter=voter_data,
            candidate=candidate.model_dump(),
        )

        try:
            self._vote_repo.insert(vote)
            self._producer.produce_model(
                topic=settings.kafka.votes_topic,
                model=vote,
                key_field="voter_id",
            )
            self._producer.poll(0)
            logger.info("Voted: voter=%s -> candidate=%s", vote.voter_id, vote.candidate_id)
        except IntegrityError:
            logger.warning("Voter %s has already voted, skipping.", vote.voter_id)
        except Exception:
            logger.exception("Failed to process vote for voter=%s", vote.voter_id)
