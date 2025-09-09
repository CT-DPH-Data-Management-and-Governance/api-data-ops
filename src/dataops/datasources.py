from abc import ABC, abstractmethod


class ResponseData(ABC):
    """Response Data from the API"""

    pass


class DataSource(ABC):
    """Abstract base class defining the contract for any data sources."""

    @abstractmethod
    def fetch(url: str) -> ResponseData:
        """Fetch raw data from a given URL."""
        pass
