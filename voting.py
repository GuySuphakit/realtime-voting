import logging
import random

from config import settings
from database.repositories import CandidateRepository, VoteRepository
from kafka_utils.consumer import KafkaConsumerWrapper
from kafka_utils.producer import KafkaProducerWrapper
from services.voting_service import VotingService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

random.seed(settings.app.random_seed)


if __name__ == "__main__":
    candidates = CandidateRepository().get_all()
    if not candidates:
        raise RuntimeError("No candidates found in database. Run main.py first.")

    logger.info("Loaded %d candidates.", len(candidates))

    consumer = KafkaConsumerWrapper(topics=settings.kafka.voters_topic)
    producer = KafkaProducerWrapper()
    vote_repo = VoteRepository()

    service = VotingService(
        candidates=candidates,
        vote_repo=vote_repo,
        consumer=consumer,
        producer=producer,
    )
    service.run()
