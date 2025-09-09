from pydantic import BaseModel
from dataops.datasources import DataSource, ResponseData


class CensusAPIDataSource(BaseModel, DataSource):
    url: str

    def fetch(self, url: str) -> ResponseData:
        pass
