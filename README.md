# DMAG Data Ops


## Overview

Python toolkit for DPH DMAG Data OPS. Currently focusing on pipeline
work surrounding census apis.

## Features

- ACS API tooling:
  - [Polars](https://pola.rs/): Blazingly fast data manipulations
  - Multiple attributes and methods for keeping endpoint data as raw or
    as wrangled as you like, with varying format options!
- Strong Base for Applications:
  - [Pydantic](https://docs.pydantic.dev/latest/) models for powerful
    validation features and a smoother developer experience.
  - [Pydantic Settings
    Management](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
    for validating and handling environmental variables, build stateless
    apps with ease!

## Usage

### American Community Survey

Parse useful metadata from American Community Survey (ACS) API endpoints
even before making a single GET request:

#### Endpoints

Look at all the info you can gather before sending a request on up. Of
note is the `variable_endpoint` so you can scope out the variable labels
yourself if you like.

``` python
from dataops.apis.acs import APIData, APIEndpoint
import polars as pl

acs_url = (
    "https://api.census.gov/data/2022/acs/acs1?get=group(B19013I)&ucgid=0400000US09"
)
endpoint = APIEndpoint.from_url(acs_url)

endpoint
```

    APIEndpoint(
        dataset='acs/acs1',
        base_url='https://api.census.gov/data', 
        table_type='detailed', 
        year='2022', 
        variables='['group(B19013I)']', 
        group='B19013I', 
        geography='ucgid:0400000US09', 
        url_no_key='https://api.census.gov/data/2022/acs/acs1?get=group%28B19013I%29&ucgid=0400000US09', 
        variable_endpoint='https://api.census.gov/data/2022/acs/acs1/groups/B19013I',
    )

#### Grabbing Data

Take that endpoint and turn it into data.

``` python
data = APIData(endpoint=endpoint)
data
```

    APIData(
        endpoint='https://api.census.gov/data/2022/acs/acs1?get=group%28B19013I%29&ucgid=0400000US09',
        concept/s='['Median Household Income in the Past 12 Months (in 2022 Inflation-Adjusted Dollars) (Hispanic or Latino Householder)']'

There are various options for looking at the results. Here is the raw
results from the API.

``` python
# raw data
data._raw
```

    [['B19013I_001E',
      'B19013I_001EA',
      'B19013I_001M',
      'B19013I_001MA',
      'GEO_ID',
      'NAME',
      'ucgid'],
     ['60275', None, '4773', None, '0400000US09', 'Connecticut', '0400000US09']]

When the raw data is grabbed, basic foundational wrangles are performed
in polars and then the lazyframes are cached into memory. Once the
`APIData` object is created and saved there is no need to call that
endpoint again since you’ll get the raw result and many of the
intermediary steps in the polishing process.

Here is a long format that might be good for storing in a database:

``` python
# wrangled data
data.long().head().collect()
```

<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (5, 13)</small>

| stratifier_id | row_id | measure_id | universe | concept | measure | value_type | value | variable | endpoint | year | dataset | date_pulled |
|----|----|----|----|----|----|----|----|----|----|----|----|----|
| u32 | u32 | i64 | str | str | str | str | str | str | str | i32 | str | datetime\[μs\] |
| 0 | 0 | 1 | "Households with a householder … | "Median Household Income in the… | "estimate" | "estimate" | "60275" | "B19013I_001E" | "https://api.census.gov/data/20… | 2022 | "acs/acs1" | 2025-08-13 10:49:18.824543 |
| 0 | 1 | 1 | "Households with a householder … | "Median Household Income in the… | "annotation of estimate" | "annotation of estimate" | null | "B19013I_001EA" | "https://api.census.gov/data/20… | 2022 | "acs/acs1" | 2025-08-13 10:49:18.824543 |
| 0 | 2 | 1 | "Households with a householder … | "Median Household Income in the… | "margin of error" | "margin of error" | "4773" | "B19013I_001M" | "https://api.census.gov/data/20… | 2022 | "acs/acs1" | 2025-08-13 10:49:18.824543 |
| 0 | 3 | 1 | "Households with a householder … | "Median Household Income in the… | "annotation of margin of error" | "annotation of margin of error" | null | "B19013I_001MA" | "https://api.census.gov/data/20… | 2022 | "acs/acs1" | 2025-08-13 10:49:18.824543 |
| 0 | 4 | null | "Households with a householder … | "Median Household Income in the… | "GEO_ID" | "GEO_ID" | "0400000US09" | "GEO_ID" | "https://api.census.gov/data/20… | 2022 | "acs/acs1" | 2025-08-13 10:49:18.824543 |

</div>

This wide format is better served for analysis:

``` python
# wrangled data
data.wide().collect()
```

<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (1, 16)</small>

| row_id | stratifier_id | name | geo_id | ucgid | universe | concept | measure | estimate | annotation of estimate | margin of error | annotation of margin of error | endpoint | year | dataset | date_pulled |
|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|
| u32 | u32 | str | str | str | str | str | str | str | str | str | str | str | i32 | str | datetime\[μs\] |
| 0 | 0 | "Connecticut" | "0400000US09" | "0400000US09" | "Households with a householder … | "Median Household Income in the… | "median household income in the… | "60275" | null | "4773" | null | "https://api.census.gov/data/20… | 2022 | "acs/acs1" | 2025-08-13 10:49:18.824543 |

</div>

### Environmental Variables for Stateless Apps

There are several reusable basesettings and configuration models that
when called will grab the variables from the environment or a .env file.
This allows for a near seamless transition from local dev to production.

## Repo Setup

### Github Actions

`Ruff` is used to check the project on a push or pull request.

### Pre-Commit

[pre-commit](https://pre-commit.com/) is used and configured to have
`Ruff` fix and format code in a commit.
