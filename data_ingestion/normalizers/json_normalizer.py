"""JSON normalizer that converts raw collector output into a canonical schema."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from data_ingestion.config.settings import DOMAIN_KEYWORDS

logger = logging.getLogger(__name__)

CANONICAL_SCHEMA_KEYS = (
    "id",
    "source",
    "source_type",
    "domain",
    "title",
    "url",
    "content",
    "published_at",
    "collected_at",
    "tags",
    "metadata",
)


class JSONNormalizer:
    """Normalize heterogeneous raw records into a unified JSON schema.

    The canonical schema produced by ``normalize`` is::

        {
            "id": "<sha256 or uuid>",
            "source": "<origin URL or name>",
            "source_type": "rss | api | web_scrape",
            "domain": "<geopolitics | economics | ...>",
            "title": "<headline>",
            "url": "<permalink>",
            "content": "<cleaned text body>",
            "published_at": "<ISO-8601 or null>",
            "collected_at": "<ISO-8601>",
            "tags": ["<keyword>", ...],
            "metadata": { ... }
        }
    """

    def normalize(self, raw_record: dict[str, Any]) -> dict[str, Any]:
        """Return a canonical dictionary from a raw collector record."""
        domain = raw_record.get("domain", "unknown")
        content = raw_record.get("raw_content", "")
        title = raw_record.get("title", "")

        tags = self._extract_tags(domain, f"{title} {content}")

        return {
            "id": raw_record.get("id", uuid.uuid4().hex),
            "source": raw_record.get("source", ""),
            "source_type": raw_record.get("source_type", "unknown"),
            "domain": domain,
            "title": title,
            "url": raw_record.get("url", ""),
            "content": content,
            "published_at": raw_record.get("published_at"),
            "collected_at": raw_record.get(
                "collected_at", datetime.now(timezone.utc).isoformat()
            ),
            "tags": tags,
            "metadata": raw_record.get("metadata", {}),
        }

    def normalize_batch(
        self, records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Normalize a batch of raw records."""
        normalized: list[dict[str, Any]] = []
        for record in records:
            try:
                normalized.append(self.normalize(record))
            except Exception:
                logger.exception("Failed to normalize record: %s", record.get("id"))
        return normalized

    @staticmethod
    def _extract_tags(domain: str, text: str) -> list[str]:
        """Match domain keywords found in the text."""
        keywords = DOMAIN_KEYWORDS.get(domain, [])
        text_lower = text.lower()
        return [kw for kw in keywords if kw.lower() in text_lower]
