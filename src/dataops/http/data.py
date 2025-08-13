import sys

import requests
from pydantic import HttpUrl


def get(endpoint: HttpUrl | str, name: str) -> dict | list:
    # check the response
    try:
        response = requests.get(endpoint, timeout=30)
        response.raise_for_status()

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred for {name}: {http_err} | Content: {response.text}")
        sys.exit(1)

    except Exception as e:
        print(f"An unexpected error occurred for {name}: {e}")
        sys.exit(1)

    # check the json deserialization
    try:
        data = response.json()

    except requests.exceptions.JSONDecodeError as json_err:
        print(f"JSON Decode error occurred for {name}: {json_err}")
        sys.exit(1)

    except Exception as e:
        print(f"An unexpected error occurred for {name}: {e}")
        sys.exit(1)

    return data
