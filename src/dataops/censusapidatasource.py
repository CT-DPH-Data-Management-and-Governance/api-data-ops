from typing import Collection
from pydantic import BaseModel
from dataops.datasources import DataSource


class CensusAPIDataSource(BaseModel, DataSource):
    url: str

    def fetch(self, url: str) -> Collection:
        pass
