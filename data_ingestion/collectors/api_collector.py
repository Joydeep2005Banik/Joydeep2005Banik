"""Collector for REST API sources."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import requests

from data_ingestion.collectors.base_collector import BaseCollector

_REQUEST_TIMEOUT = 30


class APICollector(BaseCollector):
    """Fetch data from a REST API endpoint."""

    def __init__(
        self,
        name: str,
        url: str,
        domain: str,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(source_name=name, domain=domain)
        self.url = url
        self.params = params or {}
        self.headers = headers or {}

    def collect(self) -> list[dict[str, Any]]:
        response = requests.get(
            self.url,
            params=self.params,
            headers=self.headers,
            timeout=_REQUEST_TIMEOUT,
        )
        response.raise_for_status()

        payload = response.json()
        raw_text = json.dumps(payload)

        record_id = hashlib.sha256(
            f"{self.url}{raw_text[:256]}".encode()
        ).hexdigest()

        return [
            {
                "id": record_id,
                "source": self.url,
                "source_type": "api",
                "domain": self.domain,
                "title": self.source_name,
                "url": self.url,
                "raw_content": raw_text,
                "collected_at": datetime.now(timezone.utc).isoformat(),
            }
        ]
