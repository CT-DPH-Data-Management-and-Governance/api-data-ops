from dataops.builders.starmodel import ACSStarModelBuilder
from dataops.apis.acs import APIData, APIEndpoint
import polars as pl

u1 = "https://api.census.gov/data/2023/acs/acs5/profile?get=group(DP05)&ucgid=pseudo(0400000US09$0600000)"
u2 = "https://api.census.gov/data/2023/acs/acs1/subject?get=group(S2301)&ucgid=0400000US09"
u3 = "https://api.census.gov/data/2023/acs/acs5?get=group(B25064)&ucgid=pseudo(0400000US09$0600000)"
u4 = "https://api.census.gov/data/2023/acs/acs5?get=group(B05006)&ucgid=pseudo(0400000US09$0600000)"
u5 = "https://api.census.gov/data/2022/acs/acs1?get=group(B19013I)&ucgid=0400000US09"


urls = [u1, u2, u3, u4, u5]

url = u2

example = APIEndpoint.from_url(url)
example

data = APIData(endpoint=example)
data.long().head().collect()

builder = ACSStarModelBuilder(api_data=data)
builder._strats.collect()
builder._long.collect()

star = (
    builder.set_fact()
    .set_concept()
    .set_endpoint()
    .set_stratifiers_wide()
    .set_stratifiers_long()
    .set_valuetype()
    .set_dataset()
    .set_universe()
    .build()
)
star.fact.collect()
star.dim_concept.collect()
star.dim_stratifiers.collect()
star.dim_valuetype.collect()
star.dim_universe.collect()
star.dim_dataset.collect()
star.dim_endpoint.collect()


# now append em all and figure it out
def url_to_long(url: str) -> pl.LazyFrame:
    endpoint = APIEndpoint.from_url(url)
    return APIData(endpoint=endpoint).long().collect().lazy()


all_frames = []

for url in urls:
    data = url_to_long(url)
    all_frames.append(data)

longest = pl.concat(all_frames)


builder = ACSStarModelBuilder(api_data=longest)

builder._strats.collect()
builder._starter.collect()

var_values = builder._strats.select(
    ["value", "variable", "endpoint_based_strat_id"]
).collect()
variable_sets = (
    var_values.select(["variable", "endpoint_based_strat_id"])
    .sort(["endpoint_based_strat_id", "variable"])
    .group_by("endpoint_based_strat_id", maintain_order=True)
    .agg(pl.col("variable").unique(maintain_order=True).alias("variable_set"))
    .with_columns(pl.struct("variable_set").rank("dense").alias("variable_set_id"))
)


builder._strats.select(["value", "variable", "DimStratifierID"]).collect()

# okay join up variable set, group by variable set, and grab unique values

builder._long.collect()

star = (
    builder.set_fact()
    .set_concept()
    .set_endpoint()
    # .set_stratifiers_wide()
    .set_stratifiers_long()
    .set_valuetype()
    .set_dataset()
    .set_universe()
    .build()
)
star.fact.collect()
star.dim_concept.collect()
star.dim_stratifiers.collect()
star.dim_valuetype.collect()
star.dim_universe.collect()
star.dim_dataset.collect()
star.dim_endpoint.collect()


# usage
builder = ACSStarModelBuilder()
starmodel = builder.set_acs_data(data).build()


builder = ACSStarModelBuilder.from_APIData(data=example_data)
builder.build()

(
    data.long()
    .collect()
    .filter(pl.col("measure_id").is_null())
    .select(
        pl.col("stratifier_id").alias("DimStratifierID"),
        pl.col("variable"),
        pl.col("value"),
    )
    .unique()
    .pivot(on="variable", index="DimStratifierID", values="value")
)

(
    data.long()
    .collect()
    .with_columns(pl.col("universe").rank("dense").alias("DimUniverseID"))
    .select(["universe", "DimUniverseID"])
)


(starter.select(["measure_id", "endpoint_based_strat_id"]).drop_nulls().unique())

# ugly
(
    starter.filter(pl.col("measure_id").is_null())
    .select(["variable", "value"])
    .unique()
    .sort(["variable", "value"])
    .with_row_index(name="DimStratifierID", offset=1)
    .select(
        pl.col("DimStratifierID"),
        pl.col("variable").alias("stratifier_type"),
        pl.col("value").alias("stratifier_value"),
    )
)
