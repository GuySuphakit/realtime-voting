"""Generic Kafka consumer wrapper.

Provides a clean interface for consuming messages from Kafka topics with
built-in JSON deserialization and error handling.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Generator, List, Optional, Union

from confluent_kafka import Consumer, KafkaError, KafkaException

from config import settings
from kafka_utils.exceptions import ConsumerError
from kafka_utils.serializers import deserialize_from_json

logger = logging.getLogger(__name__)


class KafkaConsumerWrapper:
    """Generic Kafka consumer with JSON deserialization.

    Wraps confluent_kafka's Consumer providing:
    - Automatic JSON deserialization of values
    - Generator-based message iteration
    - Configurable polling timeout
    - Graceful shutdown
    - Context manager protocol

    Usage:
        # Basic iteration
        consumer = KafkaConsumerWrapper(['voters_topic'])
        for message in consumer.consume():
            process(message)

        # With context manager
        with KafkaConsumerWrapper(['votes_topic']) as consumer:
            for message in consumer.consume(max_messages=100):
                process(message)
    """

    def __init__(
        self,
        topics: Union[str, List[str]],
        group_id: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        auto_commit: bool = False,
    ):
        """Initialize Kafka consumer.

        Args:
            topics: Topic or list of topics to subscribe to
            group_id: Consumer group ID. If None, uses settings default.
            config: Optional custom config overrides
            auto_commit: Whether to auto-commit offsets (default False)
        """
        if isinstance(topics, str):
            topics = [topics]
        self._topics = topics

        consumer_config = settings.kafka.get_consumer_config(group_id)

        if auto_commit:
            consumer_config["enable.auto.commit"] = True

        if config:
            consumer_config.update(config)

        self._consumer = Consumer(consumer_config)
        self._consumer.subscribe(topics)
        self._running = True

    def poll(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """Poll for a single message.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            Deserialized message value, or None if no message

        Raises:
            ConsumerError: If polling fails
        """
        try:
            msg = self._consumer.poll(timeout=timeout)

            if msg is None:
                return None

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    return None
                else:
                    raise ConsumerError(
                        f"Consumer error: {msg.error()}",
                        topic=msg.topic(),
                    )

            return deserialize_from_json(msg.value())

        except KafkaException as e:
            raise ConsumerError(
                f"Kafka error during poll: {e}",
                original_error=e,
            ) from e

    def poll_raw(self, timeout: float = 1.0) -> Optional[Any]:
        """Poll for a raw message (confluent_kafka Message object).

        Use when you need access to metadata (partition, offset, headers).

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            Raw confluent_kafka Message, or None
        """
        try:
            msg = self._consumer.poll(timeout=timeout)
            if msg is None or msg.error():
                return None
            return msg
        except KafkaException as e:
            raise ConsumerError(f"Kafka error: {e}", original_error=e) from e

    def consume(
        self,
        max_messages: Optional[int] = None,
        timeout: float = 1.0,
    ) -> Generator[Dict[str, Any], None, None]:
        """Generator that yields messages continuously.

        Args:
            max_messages: Maximum messages to consume (None for unlimited)
            timeout: Polling timeout in seconds

        Yields:
            Deserialized message dictionaries
        """
        count = 0

        while self._running:
            if max_messages is not None and count >= max_messages:
                break

            message = self.poll(timeout)
            if message is not None:
                count += 1
                yield message

    def consume_batch(
        self,
        batch_size: int,
        timeout: float = 1.0,
    ) -> List[Dict[str, Any]]:
        """Consume a batch of messages.

        Args:
            batch_size: Number of messages to collect
            timeout: Per-message polling timeout

        Returns:
            List of deserialized messages (may be less than batch_size)
        """
        batch = []
        for message in self.consume(max_messages=batch_size, timeout=timeout):
            batch.append(message)
        return batch

    def commit(self, async_commit: bool = False) -> None:
        """Commit current offsets.

        Args:
            async_commit: If True, commit asynchronously
        """
        self._consumer.commit(asynchronous=async_commit)

    def stop(self) -> None:
        """Signal the consumer to stop consuming."""
        self._running = False

    def close(self) -> None:
        """Close the consumer and release resources."""
        self._running = False
        self._consumer.close()

    def __enter__(self) -> KafkaConsumerWrapper:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - closes consumer."""
        self.close()


class StreamlitKafkaConsumer:
    """Kafka consumer optimized for Streamlit dashboards.

    Uses confluent_kafka (same library as the rest of the codebase) to poll
    all available messages from a topic from the earliest offset.
    """

    def __init__(self, topic: str, config: Optional[Dict[str, Any]] = None):
        """Initialize Streamlit-compatible consumer.

        Args:
            topic: Topic to consume from
            config: Optional config overrides
        """
        consumer_config = {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "group.id": f"streamlit-{topic}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }

        if config:
            consumer_config.update(config)

        self._consumer = Consumer(consumer_config)
        self._consumer.subscribe([topic])
        self._topic = topic

    def fetch_all(self, timeout_ms: int = 1000) -> List[Dict[str, Any]]:
        """Fetch all currently available messages within timeout.

        Polls until no new messages arrive within the timeout window.

        Args:
            timeout_ms: Per-poll timeout in milliseconds

        Returns:
            List of deserialized message values
        """
        timeout_s = timeout_ms / 1000
        data = []

        while True:
            msg = self._consumer.poll(timeout=timeout_s)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                raise ConsumerError(f"Consumer error: {msg.error()}", topic=self._topic)
            data.append(deserialize_from_json(msg.value()))

        return data

    def close(self) -> None:
        """Close the consumer."""
        self._consumer.close()


def create_consumer(
    topics: Union[str, List[str]],
    for_streamlit: bool = False,
    **kwargs,
) -> Union[KafkaConsumerWrapper, StreamlitKafkaConsumer]:
    """Factory function to create appropriate consumer type.

    Args:
        topics: Topic(s) to consume
        for_streamlit: If True, creates StreamlitKafkaConsumer
        **kwargs: Additional arguments passed to consumer

    Returns:
        Appropriate consumer instance
    """
    if for_streamlit:
        if isinstance(topics, list):
            topics = topics[0]
        return StreamlitKafkaConsumer(topics, **kwargs)
    else:
        return KafkaConsumerWrapper(topics, **kwargs)
