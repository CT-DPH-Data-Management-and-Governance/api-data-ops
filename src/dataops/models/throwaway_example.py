from dataops.models.acs import APIData, APIEndpoint

from datetime import datetime as dt
import polars as pl

u1 = "https://api.census.gov/data/2023/acs/acs5?get=group(B05006)&ucgid=pseudo(0400000US09$0600000)"
u2 = "https://api.census.gov/data/2023/acs/acs1/subject?get=group(S2301)&ucgid=0400000US09"
u3 = "https://api.census.gov/data/2023/acs/acs5/profile?get=group(DP05)&ucgid=pseudo(0400000US09$0600000)"
u4 = "https://api.census.gov/data/2022/acs/acs1?get=group(B19013I)&ucgid=0400000US09"

example = APIEndpoint.from_url(u1)
example_data = APIData(endpoint=example)

pl.DataFrame(example_data._raw[1:], schema=example_data._raw[0], orient="row")

example = APIEndpoint.from_url(u2)
example_data = APIData(endpoint=example)

pl.DataFrame(example_data._raw[1:], schema=example_data._raw[0], orient="row")


example = APIEndpoint.from_url(u3)
example_data = APIData(endpoint=example)

pl.DataFrame(example_data._raw[1:], schema=example_data._raw[0], orient="row")


example = APIEndpoint.from_url(u4)
example_data = APIData(endpoint=example)

pl.DataFrame(example_data._raw[1:], schema=example_data._raw[0], orient="row")


raw = example_data._raw

(
    pl.LazyFrame(data=raw[1:], schema=raw[0], orient="row")
    .with_columns(date_pulled=dt.now())
    .with_row_index(name="stratifier_id")
    .unpivot(index="stratifier_id", value_name="value", variable_name="variable")
    .with_row_index("row_id")
    .collect()
)


example_data._lazyframe.collect()
