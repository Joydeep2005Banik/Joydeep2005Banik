"""Message serializer for Kafka-ready byte payloads."""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class MessageSerializer:
    """Serialize normalized records into UTF-8 JSON bytes for Kafka.

    Produces a ``(key_bytes, value_bytes)`` tuple where:
    * *key* = the record ``id`` encoded as UTF-8 bytes
    * *value* = the full record encoded as compact JSON UTF-8 bytes
    """

    @staticmethod
    def serialize(record: dict[str, Any]) -> tuple[bytes, bytes]:
        """Serialize a single normalized record.

        Returns:
            A ``(key, value)`` tuple of bytes.
        """
        key = record.get("id", "").encode("utf-8")
        value = json.dumps(record, ensure_ascii=False, default=str).encode(
            "utf-8"
        )
        return key, value

    @staticmethod
    def deserialize(key_bytes: bytes, value_bytes: bytes) -> dict[str, Any]:
        """Deserialize bytes back into a Python dictionary.

        Returns:
            The deserialized record dictionary.
        """
        return json.loads(value_bytes.decode("utf-8"))
