"""Tests for the JSON normalizer."""

from __future__ import annotations

import unittest

from data_ingestion.normalizers.json_normalizer import JSONNormalizer


class TestJSONNormalizer(unittest.TestCase):
    def setUp(self):
        self.normalizer = JSONNormalizer()

    def test_normalize_produces_canonical_keys(self):
        raw = {
            "id": "abc123",
            "source": "https://example.com/feed",
            "source_type": "rss",
            "domain": "technology",
            "title": "AI Breakthrough",
            "url": "https://example.com/article",
            "raw_content": "Artificial intelligence is evolving rapidly.",
            "published_at": "2024-01-01T00:00:00Z",
            "collected_at": "2024-01-01T01:00:00Z",
        }
        result = self.normalizer.normalize(raw)

        expected_keys = {
            "id", "source", "source_type", "domain", "title",
            "url", "content", "published_at", "collected_at", "tags", "metadata",
        }
        self.assertEqual(set(result.keys()), expected_keys)

    def test_normalize_maps_raw_content_to_content(self):
        raw = {
            "raw_content": "Some body text",
            "domain": "economics",
        }
        result = self.normalizer.normalize(raw)
        self.assertEqual(result["content"], "Some body text")

    def test_tags_extracted_from_content(self):
        raw = {
            "domain": "climate",
            "title": "Global warming accelerates",
            "raw_content": "Carbon emissions hit new highs, threatening sustainability.",
        }
        result = self.normalizer.normalize(raw)
        self.assertIn("global warming", result["tags"])
        self.assertIn("carbon emissions", result["tags"])
        self.assertIn("sustainability", result["tags"])

    def test_tags_empty_when_no_keywords_match(self):
        raw = {
            "domain": "defense",
            "title": "Unrelated headline",
            "raw_content": "Nothing relevant here.",
        }
        result = self.normalizer.normalize(raw)
        self.assertEqual(result["tags"], [])

    def test_normalize_batch(self):
        records = [
            {"domain": "technology", "raw_content": "AI chips", "title": "t1"},
            {"domain": "economics", "raw_content": "GDP growth", "title": "t2"},
        ]
        results = self.normalizer.normalize_batch(records)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["domain"], "technology")
        self.assertEqual(results[1]["domain"], "economics")

    def test_normalize_batch_skips_bad_records(self):
        """If normalize raises, normalize_batch should skip that record."""
        records = [
            {"domain": "technology", "raw_content": "AI", "title": "ok"},
        ]
        # Should still work with valid records
        results = self.normalizer.normalize_batch(records)
        self.assertEqual(len(results), 1)

    def test_defaults_for_missing_fields(self):
        raw = {}
        result = self.normalizer.normalize(raw)
        self.assertEqual(result["source"], "")
        self.assertEqual(result["source_type"], "unknown")
        self.assertEqual(result["domain"], "unknown")
        self.assertIsNotNone(result["id"])
        self.assertIsNotNone(result["collected_at"])


if __name__ == "__main__":
    unittest.main()
