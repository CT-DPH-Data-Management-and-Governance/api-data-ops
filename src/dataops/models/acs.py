from typing import Annotated, List
from urllib.parse import parse_qs, urlparse

import polars as pl
from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    SecretStr,
    ValidationError,
)
from pydantic_settings import SettingsConfigDict

from dataops.models.acs_mixins import APIDataMixin, APIEndpointMixin, APIRequestMixin


class APIEndpoint(APIEndpointMixin, BaseModel):
    """
    A Pydantic model to represent, validate, and interact with a
    U.S. Census Bureau's American Community Survey API endpoint.
    """

    # Core Endpoint Components
    base_url: HttpUrl | str = Field(
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

    api_key: Annotated[
        SecretStr | None,
        Field(
            repr=False,
            description="Your Census API key. If not provided, it's sourced from the CENSUS_API_KEY environment variable.",
        ),
    ] = None

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


class APIData(APIRequestMixin, APIDataMixin, BaseModel):
    """
    A Pydantic model to represent the response data
    from the Census Bureau API Endpoint.
    """

    endpoint: Annotated[APIEndpoint, Field(description="Census API endpoint")]

    model_config = SettingsConfigDict(arbitrary_types_allowed=True)

    def tidy_wide(self) -> pl.DataFrame:
        """
        generate a tidy wide table

        """
        pass

    def standard_parse(self) -> pl.LazyFrame:
        """
        Generate a tidy Polars DataFrame by removing extra rows and adding
        geography information.

        This method processes the LazyFrame associated with the APIData instance
        to exclude metadata or geography-related rows, adds a geography column
        based on the endpoint's geography attribute, and reindexes the rows.

        Returns:
            pl.DataFrame: A tidy Polars DataFrame with the processed data.
        """
        geos = self.endpoint.geography
        year = self.endpoint.year
        endpoint = self.endpoint.url_no_key

        content = self._lazyframe.with_columns(
            pl.lit(geos).alias("url_geography"),
            pl.col("value").cast(pl.Float32, strict=False).alias("value_numeric"),
            pl.lit(year).alias("year"),
            pl.lit(endpoint).alias("endpoint"),
        )

        parsed_labels = self._parse_label().select(
            [
                "row_id",
                "exclaim_count",
                "label_line_type",
                "label_concept_base",
                "label_stratifier",
                "label_end",
            ]
        )

        parsed_vars = (
            self._parse_vars()
            .select(
                [
                    "row_id",
                    "table_type",
                    "table_id",
                    "table_subject_id",
                    "subject_table_number",
                    "table_id_suffix",
                    "column_id",
                    "column_number",
                    "line_id",
                    "line_number",
                    "line_suffix",
                ]
            )
            .filter(pl.col("table_type").is_not_null())
        )

        output = (
            content.join(parsed_labels, how="left", on="row_id")
            .join(parsed_vars, how="left", on="row_id")
            .collect()
            .lazy()
        )

        return output

    def __repr__(self):
        return (
            f"APIData(\n\tendpoint='{self.endpoint.url_no_key}',\n"
            f"\tconcept/s='{self.concept}'"
        )
