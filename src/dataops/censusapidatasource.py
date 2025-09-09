from typing import Collection
import requests
import sys

from dataops.datasources import DataSource


class CensusAPIDataSource(DataSource):
    def fetch(self, url: str) -> Collection:
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

        except requests.exceptions.HTTPError as http_err:
            print(
                f"HTTP error occurred for {url}: {http_err} | Content: {response.text}"
            )
            sys.exit(1)

        except Exception as e:
            print(f"An unexpected error occurred for {url}: {e}")
            sys.exit(1)

        # check the json deserialization
        try:
            data = response.json()

        except requests.exceptions.JSONDecodeError as json_err:
            print(f"JSON Decode error occurred for {url}: {json_err}")
            sys.exit(1)

        except Exception as e:
            print(f"An unexpected error occurred for {url}: {e}")
            sys.exit(1)

        return data


census_source = CensusAPIDataSource()
census_source.fetch(
    "https://api.census.gov/data/2023/acs/acs1/subject?get=group(S1601)&for=state:09"
)
