"""Main ingestion pipeline that orchestrates collection → normalization → serialization → Kafka."""

from __future__ import annotations

import logging
from typing import Any

from data_ingestion.collectors.base_collector import BaseCollector
from data_ingestion.collectors.rss_collector import RSSCollector
from data_ingestion.collectors.api_collector import APICollector
from data_ingestion.config.settings import API_SOURCES, RSS_FEEDS
from data_ingestion.normalizers.json_normalizer import JSONNormalizer
from data_ingestion.streaming.kafka_producer import KafkaStreamProducer

logger = logging.getLogger(__name__)


class IngestionPipeline:
    """End-to-end pipeline:

    External Sources → Collectors → Raw Data Acquisition →
    JSON Normalization → Message Serialization → Kafka Streaming Topic
    """

    def __init__(self, kafka_producer: KafkaStreamProducer | None = None) -> None:
        self.normalizer = JSONNormalizer()
        self.kafka_producer = kafka_producer
        self._collectors: list[BaseCollector] = []

    def _build_collectors(self) -> list[BaseCollector]:
        """Instantiate collectors from configured sources."""
        collectors: list[BaseCollector] = []

        # RSS feed collectors
        for domain, urls in RSS_FEEDS.items():
            for url in urls:
                collectors.append(RSSCollector(feed_url=url, domain=domain))

        # REST API collectors
        for domain, sources in API_SOURCES.items():
            for src in sources:
                collectors.append(
                    APICollector(
                        name=src["name"],
                        url=src["url"],
                        domain=domain,
                        params=src.get("params"),
                    )
                )

        return collectors

    def run_once(self) -> dict[str, Any]:
        """Execute one full cycle of the pipeline.

        Returns a summary dictionary with counts per domain.
        """
        self._collectors = self._build_collectors()
        summary: dict[str, int] = {}

        for collector in self._collectors:
            # 1. Collect raw data
            raw_records = collector.safe_collect()
            if not raw_records:
                continue

            # 2. Normalize to canonical JSON
            normalized = self.normalizer.normalize_batch(raw_records)
            domain = collector.domain

            # 3 & 4. Serialize + send to Kafka (handled inside producer)
            if self.kafka_producer is not None:
                sent = self.kafka_producer.send_batch(normalized)
                summary[domain] = summary.get(domain, 0) + sent
            else:
                # Dry-run mode (no Kafka): just count
                summary[domain] = summary.get(domain, 0) + len(normalized)

        logger.info("Pipeline cycle complete. Summary: %s", summary)
        return summary
