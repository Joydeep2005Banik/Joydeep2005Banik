"""Entry point for the Global Ontology Engine data ingestion service.

Runs a continuous loop that:
1. Collects data from configured external sources (RSS, APIs, web scrapers).
2. Normalizes records into a canonical JSON schema.
3. Serializes and publishes them to the appropriate Kafka topic.
"""

from __future__ import annotations

import logging
import sys
import time

from data_ingestion.config.settings import COLLECTION_INTERVAL, LOG_LEVEL
from data_ingestion.pipeline.ingestion_pipeline import IngestionPipeline
from data_ingestion.streaming.kafka_producer import KafkaStreamProducer

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Start the ingestion loop."""
    logger.info("Starting Global Ontology Engine — Data Ingestion Service")

    producer = KafkaStreamProducer()
    try:
        producer.connect()
    except Exception:
        logger.exception("Could not connect to Kafka. Exiting.")
        sys.exit(1)

    pipeline = IngestionPipeline(kafka_producer=producer)

    try:
        while True:
            logger.info("Running ingestion cycle…")
            summary = pipeline.run_once()
            logger.info("Cycle summary: %s", summary)
            logger.info(
                "Sleeping %d seconds until next cycle.", COLLECTION_INTERVAL
            )
            time.sleep(COLLECTION_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Shutdown requested.")
    finally:
        producer.close()
        logger.info("Ingestion service stopped.")


if __name__ == "__main__":
    main()
