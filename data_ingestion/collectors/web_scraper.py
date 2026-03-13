"""Collector that scrapes web pages for textual content."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

from data_ingestion.collectors.base_collector import BaseCollector

_REQUEST_TIMEOUT = 30


class WebScraper(BaseCollector):
    """Scrape a web page and extract its visible text."""

    def __init__(self, page_url: str, domain: str) -> None:
        super().__init__(source_name=page_url, domain=domain)
        self.page_url = page_url

    def collect(self) -> list[dict[str, Any]]:
        response = requests.get(self.page_url, timeout=_REQUEST_TIMEOUT)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")

        # Remove script and style elements
        for tag in soup(["script", "style"]):
            tag.decompose()

        text = soup.get_text(separator=" ", strip=True)
        title = soup.title.string.strip() if soup.title and soup.title.string else ""

        record_id = hashlib.sha256(
            f"{self.page_url}{title}".encode()
        ).hexdigest()

        return [
            {
                "id": record_id,
                "source": self.page_url,
                "source_type": "web_scrape",
                "domain": self.domain,
                "title": title,
                "url": self.page_url,
                "raw_content": text,
                "collected_at": datetime.now(timezone.utc).isoformat(),
            }
        ]
