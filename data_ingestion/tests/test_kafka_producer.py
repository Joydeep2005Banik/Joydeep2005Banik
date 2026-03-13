"""Tests for the Kafka stream producer (unit tests with mocks)."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from data_ingestion.streaming.kafka_producer import KafkaStreamProducer


class TestKafkaStreamProducer(unittest.TestCase):
    def test_send_raises_when_not_connected(self):
        producer = KafkaStreamProducer()
        with self.assertRaises(RuntimeError):
            producer.send({"id": "x", "domain": "technology"})

    @patch("data_ingestion.streaming.kafka_producer.KafkaProducer")
    def test_connect_creates_producer(self, mock_kafka_cls):
        producer = KafkaStreamProducer(bootstrap_servers="localhost:9092")
        producer.connect()
        mock_kafka_cls.assert_called_once()

    @patch("data_ingestion.streaming.kafka_producer.KafkaProducer")
    def test_send_routes_to_correct_topic(self, mock_kafka_cls):
        mock_inner = MagicMock()
        mock_future = MagicMock()
        mock_inner.send.return_value = mock_future
        mock_kafka_cls.return_value = mock_inner

        producer = KafkaStreamProducer(bootstrap_servers="localhost:9092")
        producer.connect()
        producer.send({"id": "r1", "domain": "climate", "content": "data"})

        mock_inner.send.assert_called_once()
        call_args = mock_inner.send.call_args
        self.assertEqual(call_args[0][0], "raw.climate")

    @patch("data_ingestion.streaming.kafka_producer.KafkaProducer")
    def test_send_skips_unknown_domain(self, mock_kafka_cls):
        mock_inner = MagicMock()
        mock_kafka_cls.return_value = mock_inner

        producer = KafkaStreamProducer(bootstrap_servers="localhost:9092")
        producer.connect()
        producer.send({"id": "r2", "domain": "unknown_domain", "content": "x"})

        mock_inner.send.assert_not_called()

    @patch("data_ingestion.streaming.kafka_producer.KafkaProducer")
    def test_send_batch_returns_count(self, mock_kafka_cls):
        mock_inner = MagicMock()
        mock_future = MagicMock()
        mock_inner.send.return_value = mock_future
        mock_kafka_cls.return_value = mock_inner

        producer = KafkaStreamProducer(bootstrap_servers="localhost:9092")
        producer.connect()
        count = producer.send_batch([
            {"id": "a", "domain": "technology", "content": "x"},
            {"id": "b", "domain": "economics", "content": "y"},
        ])
        self.assertEqual(count, 2)

    @patch("data_ingestion.streaming.kafka_producer.KafkaProducer")
    def test_close_flushes_and_closes(self, mock_kafka_cls):
        mock_inner = MagicMock()
        mock_kafka_cls.return_value = mock_inner

        producer = KafkaStreamProducer(bootstrap_servers="localhost:9092")
        producer.connect()
        producer.close()
        mock_inner.flush.assert_called()
        mock_inner.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
