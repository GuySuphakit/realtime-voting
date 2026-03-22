"""
Centralized configuration management using environment variables.

This module provides a single source of truth for all application configuration,
following the DRY (Don't Repeat Yourself) principle. Previously, database connections,
Kafka settings, and topic names were hardcoded and duplicated across multiple files.

Usage:
    from config import settings

    # Access database configuration
    conn = psycopg2.connect(settings.database.connection_string)

    # Access Kafka configuration
    producer = SerializingProducer(settings.kafka.get_producer_config())
    topic = settings.kafka.voters_topic
"""
import os
from typing import List
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
# This allows easy configuration for different environments (dev, staging, production)
load_dotenv()


class DatabaseConfig:
    """
    Manages all PostgreSQL database connection settings.

    Reads from environment variables with fallback defaults for local development.
    Provides connection strings in multiple formats for different libraries.
    """

    def __init__(self):
        # Read database credentials from environment or use local defaults
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.port = int(os.getenv('POSTGRES_PORT', '5432'))
        self.database = os.getenv('POSTGRES_DB', 'voting')
        self.user = os.getenv('POSTGRES_USER', 'postgres')
        self.password = os.getenv('POSTGRES_PASSWORD', 'postgres')

    @property
    def connection_string(self) -> str:
        """
        Generates psycopg2-compatible connection string.

        Used by main.py, voting.py, and streamlit-app.py for database connections.
        Replaces 3 duplicate hardcoded connection strings.
        """
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"

    @property
    def jdbc_url(self) -> str:
        """
        Generates JDBC URL for Spark streaming connections.

        Used by spark-streaming.py to read/write data from PostgreSQL.
        """
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


class KafkaConfig:
    """
    Manages all Kafka messaging configuration.

    Centralizes Kafka broker addresses and topic names that were previously
    scattered across 4 different files as magic strings. Provides consistent
    configuration for both producers and consumers.
    """

    def __init__(self):
        # Kafka cluster connection settings
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.group_id = os.getenv('KAFKA_GROUP_ID', 'voting-group')
        self.auto_offset_reset = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')

        # Topic names - centralized to avoid magic strings throughout the codebase
        # Raw data topics (voter and candidate information)
        self.voters_topic = os.getenv('KAFKA_VOTERS_TOPIC', 'voters_topic')
        self.candidates_topic = os.getenv('KAFKA_CANDIDATES_TOPIC', 'candidates_topic')
        self.votes_topic = os.getenv('KAFKA_VOTES_TOPIC', 'votes_topic')

        # Aggregated data topics (processed by Spark)
        self.aggregated_votes_per_candidate_topic = os.getenv(
            'KAFKA_AGGREGATED_VOTES_TOPIC',
            'aggregated_votes_per_candidate'
        )
        self.aggregated_turnout_by_location_topic = os.getenv(
            'KAFKA_AGGREGATED_TURNOUT_TOPIC',
            'aggregated_turnout_by_location'
        )

    def get_producer_config(self) -> dict:
        """
        Returns configuration dictionary for Kafka producers.

        Used by main.py and voting.py to publish voter and vote data.
        Ensures consistent producer settings across the application.
        """
        return {
            'bootstrap.servers': self.bootstrap_servers,
        }

    def get_consumer_config(self, group_id: str = None) -> dict:
        """
        Returns configuration dictionary for Kafka consumers.

        Args:
            group_id: Optional consumer group ID. If not provided, uses default from config.

        Used by voting.py and streamlit-app.py to consume messages.
        Disables auto-commit to allow manual offset management for reliability.
        """
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': group_id or self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': False
        }


class SparkConfig:
    """
    Manages Spark Streaming configuration.

    Controls Spark execution mode, checkpoint directories for fault tolerance,
    and database driver paths. Used exclusively by spark-streaming.py for
    real-time vote aggregation.
    """

    def __init__(self):
        # Spark execution mode (local[*] uses all available CPU cores)
        self.master = os.getenv('SPARK_MASTER', 'local[*]')
        self.app_name = os.getenv('SPARK_APP_NAME', 'ElectionAnalysis')

        # Checkpoint directory for streaming fault tolerance
        # Stores state information to resume processing after failures
        self.checkpoint_dir = os.getenv('SPARK_CHECKPOINT_DIR', 'checkpoints')

        # PostgreSQL JDBC driver path for database connectivity
        # Previously hardcoded as absolute path in spark-streaming.py
        self.postgresql_jar_path = os.getenv(
            'POSTGRESQL_JAR_PATH',
            'postgresql-42.7.2.jar'
        )

        # Watermark duration for handling late-arriving events
        # Determines how long to wait for delayed vote data
        self.watermark_duration = os.getenv('SPARK_WATERMARK_DURATION', '1 minute')


class ApplicationConfig:
    """
    Manages application-specific business logic settings.

    Controls voter/candidate generation, timing parameters, and external API
    endpoints. These settings affect how the voting simulation behaves.
    """

    def __init__(self):
        # Random seed for reproducible test data generation
        self.random_seed = int(os.getenv('RANDOM_SEED', '42'))

        # Number of candidates and voters to generate
        # Used by main.py during initialization
        self.num_candidates = int(os.getenv('NUM_CANDIDATES', '3'))
        self.num_voters = int(os.getenv('NUM_VOTERS', '500'))

        # Delay between vote submissions in seconds
        # Controls voting simulation speed in voting.py
        self.voting_delay_seconds = float(os.getenv('VOTING_DELAY_SECONDS', '0.2'))

        # External API endpoint for generating realistic voter/candidate data
        # Uses randomuser.me to create demographic information
        self.randomuser_api_url = os.getenv(
            'RANDOMUSER_API_URL',
            'https://randomuser.me/api/?nat=gb'
        )

        # List of political parties for candidate assignment
        # Loaded from comma-separated string in environment
        self.parties = self._parse_parties(
            os.getenv('PARTIES', 'Management Party,Savior Party,Tech Republic Party')
        )

    @staticmethod
    def _parse_parties(parties_str: str) -> List[str]:
        """
        Converts comma-separated party names into a list.

        Args:
            parties_str: Comma-separated string like "Party A,Party B,Party C"

        Returns:
            List of party names with whitespace trimmed
        """
        return [party.strip() for party in parties_str.split(',')]


class Settings:
    """
    Aggregates all configuration categories into a single settings object.

    This is the main entry point for accessing configuration throughout the application.
    Instantiated as a singleton to ensure consistent settings across all modules.

    Usage:
        from config import settings
        print(settings.database.host)
        print(settings.kafka.bootstrap_servers)
    """

    def __init__(self):
        self.database = DatabaseConfig()
        self.kafka = KafkaConfig()
        self.spark = SparkConfig()
        self.app = ApplicationConfig()


# Singleton instance - import this directly in your code
# Ensures all modules use the same configuration without re-reading environment variables
settings = Settings()