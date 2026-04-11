"""JSON serialization utilities for Kafka messages.

Provides consistent JSON serialization/deserialization using simplejson,
which handles edge cases (Decimal, datetime) better than stdlib json.
"""

from typing import Any, Dict

import simplejson as json
from pydantic import BaseModel

from kafka_utils.exceptions import DeserializationError, SerializationError


def serialize_to_json(data: Any) -> bytes:
    """Serialize data to JSON bytes for Kafka.

    Handles:
    - Pydantic models (via model_dump())
    - Dictionaries
    - Other JSON-serializable types

    Args:
        data: Data to serialize (dict, Pydantic model, or JSON-serializable)

    Returns:
        UTF-8 encoded JSON bytes

    Raises:
        SerializationError: If serialization fails
    """
    try:
        if isinstance(data, BaseModel):
            json_str = json.dumps(data.model_dump(mode="json"))
        else:
            json_str = json.dumps(data)
        return json_str.encode("utf-8")
    except (TypeError, ValueError) as e:
        raise SerializationError(f"Failed to serialize data: {e}", data=data) from e


def deserialize_from_json(data: bytes) -> Dict[str, Any]:
    """Deserialize JSON bytes from Kafka to dictionary.

    Args:
        data: UTF-8 encoded JSON bytes

    Returns:
        Parsed dictionary

    Raises:
        DeserializationError: If deserialization fails
    """
    try:
        return json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise DeserializationError(f"Failed to deserialize data: {e}", raw_data=data) from e


def json_serializer(data: Any, context=None) -> bytes:
    """Serializer function compatible with confluent_kafka SerializingProducer.

    Can be passed directly to SerializingProducer's value.serializer config.

    Args:
        data: Data to serialize
        context: SerializationContext (unused, for API compatibility)

    Returns:
        Serialized bytes
    """
    return serialize_to_json(data)


def json_deserializer(data: bytes, context=None) -> Dict[str, Any]:
    """Deserializer function compatible with confluent_kafka consumers.

    Args:
        data: Bytes to deserialize
        context: SerializationContext (unused, for API compatibility)

    Returns:
        Deserialized dictionary
    """
    return deserialize_from_json(data)
