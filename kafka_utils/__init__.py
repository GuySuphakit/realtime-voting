"""Kafka utilities package for the realtime-voting application.

Provides:
- KafkaProducerWrapper: Generic producer with JSON serialization
- KafkaConsumerWrapper: Generic consumer with JSON deserialization
- StreamlitKafkaConsumer: Dashboard-optimized consumer
- Serializers: JSON serialization utilities

Usage:
    from kafka_utils import KafkaProducerWrapper, KafkaConsumerWrapper

    # Produce messages
    producer = KafkaProducerWrapper()
    producer.produce('topic', 'key', {'data': 'value'})
    producer.flush()

    # Consume messages
    with KafkaConsumerWrapper(['topic']) as consumer:
        for msg in consumer.consume():
            process(msg)
"""

from kafka_utils.consumer import (
    KafkaConsumerWrapper,
    StreamlitKafkaConsumer,
    create_consumer,
)
from kafka_utils.exceptions import (
    ConsumerError,
    DeserializationError,
    KafkaError,
    ProducerError,
    SerializationError,
)
from kafka_utils.producer import KafkaProducerWrapper
from kafka_utils.serializers import (
    deserialize_from_json,
    json_deserializer,
    json_serializer,
    serialize_to_json,
)

__all__ = [
    # Producer
    "KafkaProducerWrapper",
    # Consumer
    "KafkaConsumerWrapper",
    "StreamlitKafkaConsumer",
    "create_consumer",
    # Serializers
    "serialize_to_json",
    "deserialize_from_json",
    "json_serializer",
    "json_deserializer",
    # Exceptions
    "KafkaError",
    "ProducerError",
    "ConsumerError",
    "SerializationError",
    "DeserializationError",
]
