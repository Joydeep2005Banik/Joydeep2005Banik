"""Collector for RSS / Atom feeds."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

import feedparser

from data_ingestion.collectors.base_collector import BaseCollector


class RSSCollector(BaseCollector):
    """Fetch and parse entries from an RSS or Atom feed URL."""

    def __init__(self, feed_url: str, domain: str) -> None:
        super().__init__(source_name=feed_url, domain=domain)
        self.feed_url = feed_url

    def collect(self) -> list[dict[str, Any]]:
        feed = feedparser.parse(self.feed_url)
        records: list[dict[str, Any]] = []
        for entry in feed.entries:
            title = entry.get("title", "")
            link = entry.get("link", "")
            summary = entry.get("summary", "")
            published = entry.get("published", "")

            record_id = hashlib.sha256(
                f"{link}{title}".encode()
            ).hexdigest()

            records.append(
                {
                    "id": record_id,
                    "source": self.feed_url,
                    "source_type": "rss",
                    "domain": self.domain,
                    "title": title,
                    "url": link,
                    "raw_content": summary,
                    "published_at": published,
                    "collected_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        return records
