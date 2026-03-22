import logging
import random

from config import settings
from database.exceptions import IntegrityError
from database.repositories import CandidateRepository, VoterRepository, create_all_tables
from kafka_utils.producer import KafkaProducerWrapper
from services.data_generator import DataGeneratorService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

random.seed(settings.app.random_seed)


def seed_candidates(repo: CandidateRepository, generator: DataGeneratorService) -> None:
    """Insert candidates if the table is empty."""
    if repo.count() > 0:
        logger.info("Candidates already seeded, skipping.")
        return

    for i in range(settings.app.num_candidates):
        candidate = generator.generate_candidate(i)
        repo.insert(candidate)
        logger.info("Seeded candidate: %s (%s)", candidate.candidate_name, candidate.party_affiliation)


def seed_voters(
    repo: VoterRepository,
    generator: DataGeneratorService,
    producer: KafkaProducerWrapper,
) -> None:
    """Generate voters, persist to DB, and publish to Kafka."""
    for i in range(settings.app.num_voters):
        voter = generator.generate_voter()
        try:
            repo.insert(voter)
        except IntegrityError:
            logger.warning("Voter %s already exists, skipping DB insert.", voter.voter_id)

        producer.produce_model(
            topic=settings.kafka.voters_topic,
            model=voter,
            key_field="voter_id",
        )
        producer.flush()
        logger.info("Produced voter %d: %s", i, voter.voter_id)


if __name__ == "__main__":
    create_all_tables()

    generator = DataGeneratorService()
    candidate_repo = CandidateRepository()
    voter_repo = VoterRepository()

    seed_candidates(candidate_repo, generator)

    with KafkaProducerWrapper() as producer:
        seed_voters(voter_repo, generator, producer)
