from typing import Collection

# from pydantic import BaseModel
from abc import ABC, abstractmethod
import polars as pl


class DataProcessor(ABC):
    """Abstract Base Class defining the contract for processing any raw data"""

    @abstractmethod
    def to_lazyframe(self, data: Collection) -> pl.LazyFrame:
        pass


class PolarsProcessor(DataProcessor):
    """Implemenetation of polars"""

    def to_lazyframe(self, data: Collection) -> pl.LazyFrame:
        """
        Returns the API Response data back in a dataframe
        closely mirroring the raw response text, often as a
        monstrously wide table.
        """

        return pl.LazyFrame(data=data[1:], schema=data[0], orient="row")
