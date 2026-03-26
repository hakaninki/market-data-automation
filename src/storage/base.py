"""Base class for storage adapters."""

from abc import ABC, abstractmethod
from typing import List

from src.models import MarketRecord


class StorageAdapter(ABC):
    """Abstract base class for all data storage adapters."""

    @abstractmethod
    def write(self, records: List[MarketRecord]) -> None:
        """Write a list of MarketRecord instances to the underlying storage."""
        pass
