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
        # add the _Rawframe wide stuff here
        # load data

        # placeholder
        return pl.LazyFrame()
