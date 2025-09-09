from abc import ABC, abstractmethod
from typing import Collection


class DataSource(ABC):
    """Abstract base class defining the contract for any data sources."""

    @abstractmethod
    def fetch(url: str) -> Collection:
        """Fetch raw data from a given URL."""
        pass
