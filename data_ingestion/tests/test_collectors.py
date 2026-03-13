"""Tests for data collectors."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from data_ingestion.collectors.base_collector import BaseCollector
from data_ingestion.collectors.rss_collector import RSSCollector
from data_ingestion.collectors.api_collector import APICollector
from data_ingestion.collectors.web_scraper import WebScraper


# ── BaseCollector ──────────────────────────────────────────────────────

class ConcreteCollector(BaseCollector):
    """Minimal concrete implementation for testing the ABC."""

    def __init__(self, records=None, should_raise=False):
        super().__init__(source_name="test-source", domain="technology")
        self._records = records or []
        self._should_raise = should_raise

    def collect(self):
        if self._should_raise:
            raise RuntimeError("boom")
        return self._records


class TestBaseCollector(unittest.TestCase):
    def test_safe_collect_returns_records(self):
        records = [{"source": "s", "domain": "technology", "raw_content": "x"}]
        c = ConcreteCollector(records=records)
        self.assertEqual(c.safe_collect(), records)

    def test_safe_collect_returns_empty_on_error(self):
        c = ConcreteCollector(should_raise=True)
        self.assertEqual(c.safe_collect(), [])


# ── RSSCollector ───────────────────────────────────────────────────────

class TestRSSCollector(unittest.TestCase):
    @patch("data_ingestion.collectors.rss_collector.feedparser.parse")
    def test_collect_parses_entries(self, mock_parse):
        mock_entry = MagicMock()
        mock_entry.get = lambda k, d="": {
            "title": "Test Title",
            "link": "https://example.com/article",
            "summary": "A short summary.",
            "published": "Mon, 01 Jan 2024 00:00:00 GMT",
        }.get(k, d)
        mock_parse.return_value = MagicMock(entries=[mock_entry])

        collector = RSSCollector(
            feed_url="https://example.com/feed.xml",
            domain="geopolitics",
        )
        records = collector.collect()

        self.assertEqual(len(records), 1)
        rec = records[0]
        self.assertEqual(rec["source_type"], "rss")
        self.assertEqual(rec["domain"], "geopolitics")
        self.assertEqual(rec["title"], "Test Title")
        self.assertEqual(rec["url"], "https://example.com/article")
        self.assertIn("id", rec)
        self.assertIn("collected_at", rec)

    @patch("data_ingestion.collectors.rss_collector.feedparser.parse")
    def test_collect_empty_feed(self, mock_parse):
        mock_parse.return_value = MagicMock(entries=[])
        collector = RSSCollector(feed_url="https://example.com/empty", domain="economics")
        self.assertEqual(collector.collect(), [])


# ── APICollector ───────────────────────────────────────────────────────

class TestAPICollector(unittest.TestCase):
    @patch("data_ingestion.collectors.api_collector.requests.get")
    def test_collect_returns_record(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": 42}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        collector = APICollector(
            name="TestAPI",
            url="https://api.example.com/data",
            domain="economics",
        )
        records = collector.collect()

        self.assertEqual(len(records), 1)
        rec = records[0]
        self.assertEqual(rec["source_type"], "api")
        self.assertEqual(rec["domain"], "economics")
        self.assertIn('"value": 42', rec["raw_content"])

    @patch("data_ingestion.collectors.api_collector.requests.get")
    def test_collect_raises_on_http_error(self, mock_get):
        mock_get.return_value.raise_for_status.side_effect = Exception("404")
        collector = APICollector(
            name="BadAPI", url="https://api.example.com/bad", domain="defense"
        )
        with self.assertRaises(Exception):
            collector.collect()


# ── WebScraper ─────────────────────────────────────────────────────────

class TestWebScraper(unittest.TestCase):
    @patch("data_ingestion.collectors.web_scraper.requests.get")
    def test_collect_extracts_text(self, mock_get):
        html = (
            "<html><head><title>Page Title</title></head>"
            "<body><p>Hello World</p></body></html>"
        )
        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        collector = WebScraper(
            page_url="https://example.com/page",
            domain="society",
        )
        records = collector.collect()

        self.assertEqual(len(records), 1)
        rec = records[0]
        self.assertEqual(rec["source_type"], "web_scrape")
        self.assertEqual(rec["title"], "Page Title")
        self.assertIn("Hello World", rec["raw_content"])


if __name__ == "__main__":
    unittest.main()
