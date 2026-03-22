"""Custom exceptions for Kafka operations."""

from typing import Any, Optional


class KafkaError(Exception):
    """Base exception for all Kafka-related errors."""

    pass


class ProducerError(KafkaError):
    """Raised when message production fails."""

    def __init__(
        self,
        message: str,
        topic: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.topic = topic
        self.original_error = original_error


class ConsumerError(KafkaError):
    """Raised when message consumption fails."""

    def __init__(
        self,
        message: str,
        topic: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.topic = topic
        self.original_error = original_error


class SerializationError(KafkaError):
    """Raised when message serialization fails."""

    def __init__(self, message: str, data: Any = None):
        super().__init__(message)
        self.data = data


class DeserializationError(KafkaError):
    """Raised when message deserialization fails."""

    def __init__(self, message: str, raw_data: Optional[bytes] = None):
        super().__init__(message)
        self.raw_data = raw_data
