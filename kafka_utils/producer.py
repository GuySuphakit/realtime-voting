"""Generic Kafka producer wrapper.

Provides a clean interface for producing messages to Kafka topics with
built-in JSON serialization and error handling.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional, Union

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer

from config import settings
from kafka_utils.exceptions import ProducerError
from kafka_utils.serializers import json_serializer

logger = logging.getLogger(__name__)

DeliveryCallback = Callable[[Optional[Exception], Any], None]


def default_delivery_callback(err: Optional[Exception], msg: Any) -> None:
    """Default delivery callback for message acknowledgment."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")


class KafkaProducerWrapper:
    """Generic Kafka producer with JSON serialization.

    Wraps confluent_kafka's SerializingProducer providing:
    - Automatic JSON serialization of values
    - String serialization of keys
    - Configurable delivery callbacks
    - Context manager protocol

    Usage:
        # Basic usage
        producer = KafkaProducerWrapper()
        producer.produce(
            topic=settings.kafka.voters_topic,
            key=voter.voter_id,
            value=voter.model_dump()
        )
        producer.flush()

        # With context manager
        with KafkaProducerWrapper() as producer:
            producer.produce(topic, key, value)
        # Auto-flush on exit
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        on_delivery: Optional[DeliveryCallback] = None,
    ):
        """Initialize Kafka producer.

        Args:
            config: Optional custom config. If None, uses settings.kafka.get_producer_config()
            on_delivery: Optional delivery callback. If None, uses default logger.
        """
        producer_config = settings.kafka.get_producer_config()
        if config:
            producer_config.update(config)

        producer_config["key.serializer"] = StringSerializer("utf_8")
        producer_config["value.serializer"] = json_serializer

        self._producer = SerializingProducer(producer_config)
        self._on_delivery = on_delivery or default_delivery_callback

    def produce(
        self,
        topic: str,
        key: str,
        value: Union[Dict[str, Any], Any],
        on_delivery: Optional[DeliveryCallback] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """Produce a message to a Kafka topic.

        Args:
            topic: Target topic name
            key: Message key (used for partitioning)
            value: Message value (will be JSON serialized)
            on_delivery: Optional per-message delivery callback
            headers: Optional message headers

        Raises:
            ProducerError: If production fails immediately
        """
        try:
            self._producer.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=on_delivery or self._on_delivery,
                headers=headers,
            )
        except Exception as e:
            raise ProducerError(
                f"Failed to produce message to '{topic}': {e}",
                topic=topic,
                original_error=e,
            )

    def produce_model(
        self,
        topic: str,
        model: Any,
        key_field: Optional[str] = None,
        on_delivery: Optional[DeliveryCallback] = None,
    ) -> None:
        """Convenience method to produce a Pydantic model.

        Automatically extracts key from model and serializes to JSON.

        Args:
            topic: Target topic name
            model: Pydantic BaseModel instance
            key_field: Field name to use as key (default: first field ending in '_id')
            on_delivery: Optional delivery callback
        """
        data = model.model_dump()

        if key_field is None:
            for field in data.keys():
                if field.endswith("_id"):
                    key_field = field
                    break

        key = str(data.get(key_field, ""))
        self.produce(topic, key, data, on_delivery)

    def poll(self, timeout: float = 0) -> int:
        """Poll for delivery callbacks.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            Number of events processed
        """
        return self._producer.poll(timeout)

    def flush(self, timeout: float = 10.0) -> int:
        """Flush all pending messages.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            Number of messages still in queue (0 if all delivered)
        """
        return self._producer.flush(timeout)

    def __enter__(self) -> KafkaProducerWrapper:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - flushes pending messages."""
        self.flush()


_default_producer: Optional[KafkaProducerWrapper] = None


def get_producer() -> KafkaProducerWrapper:
    """Get or create the default producer instance."""
    global _default_producer
    if _default_producer is None:
        _default_producer = KafkaProducerWrapper()
    return _default_producer
