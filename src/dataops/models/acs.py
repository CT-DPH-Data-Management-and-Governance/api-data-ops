from typing import Annotated, List, Optional
from urllib.parse import parse_qs, urlparse
from functools import cached_property

from datetime import datetime as dt
import polars as pl
from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    SecretStr,
    ValidationError,
    computed_field,
)

from pydantic_settings import SettingsConfigDict

from dataops.api import _get
from dataops.models.acs_mixins import APIEndpointMixin

# ideas /todoish
# class APIVariable():
# class for subj, btable, dp etc...


class APIEndpoint(APIEndpointMixin, BaseModel):
    """
    A Pydantic model to represent, validate, and interact with a
    U.S. Census Bureau's American Community Survey API endpoint.
    """

    # Core Endpoint Components
    base_url: HttpUrl = Field(
        default="https://api.census.gov/data",
        description="The base URL for the Census ACS API.",
    )

    year: Annotated[int, Field(gt=2004, description="The survey year (e.g., 2020).")]

    dataset: Annotated[
        str, Field(description="The dataset identifier (e.g., 'acs/acs1', 'acs/acs5').")
    ]

    variables: Annotated[
        List[str],
        Field(
            min_length=1,
            description="A list of variable names to retrieve (e.g., ['NAME', 'P1_001N']).",
        ),
    ]

    geography: Annotated[
        str,
        Field(
            default="ucgid:0400000US09",
            description="The geography specification (e.g., 'state:*', 'ucgid:0400000US09').",
        ),
    ]

    api_key: Optional[SecretStr] = Field(
        repr=False,
        description="Your Census API key. If not provided, it's sourced from the CENSUS_API_KEY environment variable.",
    )

    def __repr__(self):
        return (
            f"APIEndpoint(\n\tdataset='{self.dataset}',\n"
            f"\tbase_url='{self.base_url}', \n"
            f"\ttable_type='{self.table_type.value}', \n"
            f"\tyear='{self.year}', \n"
            f"\tvariables='{self.variables}', \n"
            f"\tgroup='{self.group}', \n"
            f"\tgeography='{self.geography}', \n"
            f"\turl_no_key='{self.url_no_key}', \n"
            f"\tvariable_endpoint='{self.variable_endpoint}',\n)"
        )

    # Alternative Constructor from URL
    @classmethod
    def from_url(cls, url: str) -> "APIEndpoint":
        """Parses a full Census API URL string and creates an instance."""
        try:
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            path_parts = [
                part for part in parsed_url.path.strip("/").split("/") if part
            ]

            if path_parts[0] != "data" or len(path_parts) < 3:
                raise ValueError(
                    "URL path does not match expected '/data/{year}/{dataset...}' structure."
                )

            year = int(path_parts[1])

            dataset = "/".join(path_parts[2:])

            variables = query_params.get("get", [""])[0].split(",")

            if not variables or variables == [""]:
                raise ValueError(
                    "Could not find 'get' parameter for variables in URL query."
                )

            geo_key = next(
                (key for key in ["for", "in", "ucgid"] if key in query_params), None
            )

            if not geo_key:
                raise ValueError(
                    "Could not find a recognized geography parameter ('for', 'in', 'ucgid') in URL."
                )

            geography = f"{geo_key}:{query_params[geo_key][0]}"

            api_key = query_params.get("key", [None])[0]

            return cls(
                year=year,
                dataset=dataset,
                variables=variables,
                geography=geography,
                api_key=api_key,
            )

        except (ValueError, IndexError, TypeError) as e:
            raise ValueError(f"Failed to parse URL '{url}'. Reason: {e}") from e
        except ValidationError as e:
            raise ValueError(
                f"Parsed URL components failed validation. Reason: {e}"
            ) from e


class APIData(BaseModel):
    """
    A Pydantic model to represent the response data
    from the Census Bureau API Endpoint.
    """

    endpoint: APIEndpoint = Field(..., description="Census API endpoint")
    # response codes?
    # raw

    model_config = SettingsConfigDict(arbitrary_types_allowed=True)

    # TODO: the variables will be the computed var pull
    # this till refer to that - so no _get here
    # @computed_field
    # @property
    def concept(self) -> str:
        """Endpoint ACS Concept"""

        # TODO these all have concepts with the better var endpoint
        # TODO add a fetch raw lf and then filter to concept
        # ensure that they all get concepts
        # if self.table_type != "detailed table":
        # variable_endpoint = self.endpoint.variable_endpoint
        # dataset = self.endpoint.dataset

        # data = _get(variable_endpoint, dataset)

        # return (
        #     self.endpoint.fetch_variable_labels()
        #     .select(pl.col("concept").unique())
        #     .item()
        # )

        # else:
        # return "no_concept"
        pass

    def fetch_lazyframe(self) -> pl.LazyFrame:
        """
        Return a "non-tidy" polars LazyFrame of the
        API Endpoint data with the human-readable
        variable labels.
        """

        # ensure you have the right variables
        endpoint_vars = (
            pl.LazyFrame({"vars": self.endpoint.variables})
            .with_columns(
                pl.col("vars")
                .str.replace_all("\\(|\\)", " ")
                .str.strip_chars()
                .str.split(by=" ")
            )
            .select("vars")
            .collect()
            .explode("vars")
            .lazy()
        )

        relevant_variable_labels = self._var_labels.join(
            endpoint_vars, how="inner", left_on="group", right_on="vars"
        )

        # data
        final_col_expr = pl.col(
            [
                "variable",
                "group",
                "value",
                "label",
                "concept",
                "universe",
                "date_pulled",
            ]
        )

        data = self._raw

        # TODO check if the "shotgun" approach loses the geometry with
        # the inner join...
        if len(data) == 2:
            data = (
                pl.LazyFrame({"variable": data[0], "value": data[1]})
                .with_columns(date_pulled=dt.now())
                .join(relevant_variable_labels, how="left", on="variable")
                .select(final_col_expr)
            )

        # for greater than 2 - dictionary into polars? would that work for both?

        return data

    @computed_field
    @cached_property
    def _var_labels(self) -> pl.LazyFrame:
        """
        Fetches the human-readable variable labels
        as a list and caches it.
        """
        endpoint = self.endpoint.variable_endpoint
        data = _get(endpoint, self.endpoint.dataset)

        final_vars = ["variable", "label", "concept", "group", "universe"]

        # Cherry-picked variables pull down the entire
        # variable catalog as a list with less meta info
        if isinstance(data, list):
            data = (
                pl.from_dicts(data)
                .transpose(column_names="column_0")
                .lazy()
                .with_columns(
                    pl.col("name").alias("variable"),
                    pl.col("name").str.split("_").list.first().alias("group"),
                    pl.lit("unknown").alias("universe"),
                )
                .select(final_vars)
            )

        # targeted variable endpoint yield more meta info
        # and come back as a dictionary
        if isinstance(data, dict):
            data = (
                pl.from_dicts(data.get("variables"))
                .with_row_index(name="index")
                .unpivot(index="index")
                .lazy()
                .with_columns(pl.col("value").struct.unnest())
                .select(final_vars)
            )

        return data

    @computed_field
    @cached_property
    def _raw(self) -> list[str]:
        """
        Fetches the raw data from the API and returns
        it as a list and caches it.
        """
        endpoint = self.endpoint.full_url
        dataset = self.endpoint.dataset

        data = _get(endpoint, dataset)

        return data

    # just ripping this straight over
    # geos need a rethink though
    # TODO make work for lazyframes
    # TODO really need to reassess this...
    def ensure_column_exists(
        data: pl.LazyFrame | pl.DataFrame,
        column_name: list[str] = ["geo_id", "ucgid", "geo_name"],
        default_value: any = "unknown",
    ) -> pl.LazyFrame:
        for col_name in column_name:
            if col_name not in data.collect_schema().names().columns:
                data = data.with_columns(pl.lit(default_value).alias(col_name))

        return data

    def __repr__(self):
        return (
            f"APIData(\n\tendpoint='{self.endpoint.url_no_key}',\n"
            # f"\tresponse='{self.concept}', \n"
            # f"\traw='{self.endpoint.variable_endpoint}',\n)"
        )
