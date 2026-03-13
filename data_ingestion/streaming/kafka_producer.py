"""Kafka producer wrapper for the data ingestion pipeline."""

from __future__ import annotations

import logging
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from data_ingestion.config.settings import DOMAIN_TOPICS, KAFKA_BOOTSTRAP_SERVERS
from data_ingestion.serializers.message_serializer import MessageSerializer

logger = logging.getLogger(__name__)


class KafkaStreamProducer:
    """Thin wrapper around :class:`kafka.KafkaProducer`.

    Handles topic routing based on the record's ``domain`` field and
    delegates serialization to :class:`MessageSerializer`.
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
    ) -> None:
        self._servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self._serializer = MessageSerializer()
        self._producer: KafkaProducer | None = None

    def connect(self) -> None:
        """Create the underlying Kafka producer connection."""
        self._producer = KafkaProducer(
            bootstrap_servers=self._servers,
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1,
        )
        logger.info("Kafka producer connected to %s", self._servers)

    def send(self, record: dict[str, Any]) -> None:
        """Serialize and send a single record to the appropriate topic."""
        if self._producer is None:
            raise RuntimeError("Producer not connected. Call connect() first.")

        domain = record.get("domain", "unknown")
        topic = DOMAIN_TOPICS.get(domain)
        if topic is None:
            logger.warning("No topic mapping for domain '%s'; skipping.", domain)
            return

        key, value = self._serializer.serialize(record)
        try:
            future = self._producer.send(topic, key=key, value=value)
            future.get(timeout=10)
            logger.debug("Sent record %s to %s", record.get("id"), topic)
        except KafkaError:
            logger.exception(
                "Failed to send record %s to %s", record.get("id"), topic
            )

    def send_batch(self, records: list[dict[str, Any]]) -> int:
        """Send multiple records, returning the count of successful sends."""
        sent = 0
        for record in records:
            try:
                self.send(record)
                sent += 1
            except Exception:
                logger.exception("Error sending record %s", record.get("id"))
        if self._producer is not None:
            self._producer.flush()
        return sent

    def close(self) -> None:
        """Flush and close the Kafka producer."""
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            logger.info("Kafka producer closed.")
