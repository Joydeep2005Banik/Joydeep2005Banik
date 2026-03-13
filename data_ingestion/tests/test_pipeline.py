"""Tests for the ingestion pipeline orchestrator."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from data_ingestion.pipeline.ingestion_pipeline import IngestionPipeline


class TestIngestionPipeline(unittest.TestCase):
    def test_run_once_dry_run_no_kafka(self):
        """Without a Kafka producer the pipeline should still run (dry-run)."""
        pipeline = IngestionPipeline(kafka_producer=None)

        # Patch _build_collectors to return a fake collector
        fake_collector = MagicMock()
        fake_collector.domain = "technology"
        fake_collector.safe_collect.return_value = [
            {
                "id": "1",
                "source": "test",
                "source_type": "rss",
                "domain": "technology",
                "title": "Test",
                "raw_content": "AI content",
                "collected_at": "2024-01-01T00:00:00Z",
            }
        ]
        pipeline._build_collectors = MagicMock(return_value=[fake_collector])

        summary = pipeline.run_once()
        self.assertIn("technology", summary)
        self.assertEqual(summary["technology"], 1)

    def test_run_once_with_kafka(self):
        """With a mock Kafka producer, records should be sent."""
        mock_producer = MagicMock()
        mock_producer.send_batch.return_value = 2

        pipeline = IngestionPipeline(kafka_producer=mock_producer)

        fake_collector = MagicMock()
        fake_collector.domain = "economics"
        fake_collector.safe_collect.return_value = [
            {"id": "a", "domain": "economics", "raw_content": "GDP data", "title": "t"},
            {"id": "b", "domain": "economics", "raw_content": "Trade data", "title": "t"},
        ]
        pipeline._build_collectors = MagicMock(return_value=[fake_collector])

        summary = pipeline.run_once()
        self.assertEqual(summary["economics"], 2)
        mock_producer.send_batch.assert_called_once()

    def test_run_once_skips_empty_collectors(self):
        pipeline = IngestionPipeline(kafka_producer=None)
        fake_collector = MagicMock()
        fake_collector.domain = "defense"
        fake_collector.safe_collect.return_value = []
        pipeline._build_collectors = MagicMock(return_value=[fake_collector])

        summary = pipeline.run_once()
        self.assertNotIn("defense", summary)

    def test_build_collectors_creates_instances(self):
        pipeline = IngestionPipeline()
        collectors = pipeline._build_collectors()
        self.assertGreater(len(collectors), 0)


if __name__ == "__main__":
    unittest.main()
