"""Tests for message serialization."""

from __future__ import annotations

import json
import unittest

from data_ingestion.serializers.message_serializer import MessageSerializer


class TestMessageSerializer(unittest.TestCase):
    def setUp(self):
        self.serializer = MessageSerializer()

    def test_serialize_produces_bytes(self):
        record = {
            "id": "abc123",
            "source": "test",
            "domain": "technology",
            "content": "hello",
        }
        key, value = self.serializer.serialize(record)
        self.assertIsInstance(key, bytes)
        self.assertIsInstance(value, bytes)

    def test_serialize_key_is_record_id(self):
        record = {"id": "myid", "content": "data"}
        key, _ = self.serializer.serialize(record)
        self.assertEqual(key, b"myid")

    def test_serialize_value_is_valid_json(self):
        record = {"id": "r1", "domain": "climate", "content": "test"}
        _, value = self.serializer.serialize(record)
        parsed = json.loads(value.decode("utf-8"))
        self.assertEqual(parsed["id"], "r1")
        self.assertEqual(parsed["domain"], "climate")

    def test_roundtrip(self):
        record = {"id": "rt", "domain": "defense", "content": "payload", "score": 99}
        key, value = self.serializer.serialize(record)
        restored = self.serializer.deserialize(key, value)
        self.assertEqual(restored, record)

    def test_unicode_content(self):
        record = {"id": "u1", "content": "日本語テスト"}
        key, value = self.serializer.serialize(record)
        restored = self.serializer.deserialize(key, value)
        self.assertEqual(restored["content"], "日本語テスト")


if __name__ == "__main__":
    unittest.main()
