"""Abstract base class for all data collectors."""

import abc
import logging
from typing import Any

logger = logging.getLogger(__name__)


class BaseCollector(abc.ABC):
    """Base class that every collector must extend.

    Each collector is responsible for fetching raw data from a single
    external source type (RSS, REST API, web page, etc.).
    """

    def __init__(self, source_name: str, domain: str) -> None:
        self.source_name = source_name
        self.domain = domain
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abc.abstractmethod
    def collect(self) -> list[dict[str, Any]]:
        """Fetch raw records from the external source.

        Returns a list of dictionaries, where each dictionary represents
        one raw data record with at minimum the keys:
            - ``source``: identifier for the source
            - ``domain``: topic domain
            - ``raw_content``: the unprocessed payload
        """

    def safe_collect(self) -> list[dict[str, Any]]:
        """Wrapper that catches and logs errors during collection."""
        try:
            records = self.collect()
            self.logger.info(
                "Collected %d records from %s [%s]",
                len(records),
                self.source_name,
                self.domain,
            )
            return records
        except Exception:
            self.logger.exception(
                "Error collecting from %s [%s]",
                self.source_name,
                self.domain,
            )
            return []
