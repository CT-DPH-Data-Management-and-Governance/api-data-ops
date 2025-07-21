from typing import Annotated, List
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
from dataops._helpers import _ensure_column_exists

# ideas /todoish
# class APIVariable():


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


class APIData(BaseModel):
    """
    A Pydantic model to represent the response data
    from the Census Bureau API Endpoint.
    """

    endpoint: Annotated[APIEndpoint, Field(description="Census API endpoint")]
    # response codes?
    # raw

    model_config = SettingsConfigDict(arbitrary_types_allowed=True)

    @computed_field
    @cached_property
    def concept(self) -> str:
        """Endpoint ACS Concept"""

        return (
            self._lazyframe.with_columns(
                pl.col("variable").str.split("_").list.first().alias("first")
            )
            .filter(pl.col("first").eq(pl.col("group")))
            .select(pl.col("concept"))
            .unique()
            .drop_nulls()
            .select(pl.col("concept").implode())
            .collect()
            .item()
            .to_list()
        )

    def _parse_label(self) -> pl.LazyFrame:
        origin = self._no_extra

        common_label_parts = [
            "label_line_type",
            "label_concept_base",
            "label_stratifier",
            "label_end",
        ]

        output = (
            origin.with_columns(
                pl.col("label")
                .str.count_matches("!!", literal=True)
                .alias("exclaim_count"),
                pl.col("label")
                .str.split_exact("!!", 3)
                .struct.rename_fields(common_label_parts)
                .alias("parts"),
            )
            .unnest("parts")
            .with_columns(
                pl.col(common_label_parts)
                .str.replace_all(r"--|:", "")
                .str.strip_chars()
                .str.to_lowercase()
            )
        )

        return output

    def _parse_vars(self) -> pl.LazyFrame:
        """
        Parse all the information/ metadata from the variable
        name/id based on census documentation.
        """

        # polar expressions
        table_type_expr = pl.col("variable").str.slice(0, 1).alias("table_type")

        common_var_meta_expr = (
            pl.col("table_id").str.slice(0, 1).alias("table_type"),
            pl.col("table_id").str.slice(1, 2).alias("table_subject_id"),
            pl.col("table_id").str.slice(3, 3).alias("subject_table_number"),
            pl.col("table_id").str.slice(6).alias("table_id_suffix"),
        )

        common_line_expr = (
            pl.col("line_id").str.slice(0, 3).alias("line_number").str.to_integer(),
            pl.col("line_id").str.slice(3).alias("line_suffix"),
        )

        final_vars = [
            "row_id",
            "variable",
            "group",
            "value",
            "label",
            "concept",
            "universe",
            "date_pulled",
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

        # TODO for unknown enum, like multi variable pukks
        # consider just finding table type per row on the fly?
        # and for total unknowns for variable meta info -
        # count the number of _'s?

        # should we just do that for all of them and
        # forgo the enum:
        origin = self._no_extra

        if self.endpoint.table_type.value == "subject":
            split_vars = (
                origin.with_columns(
                    pl.col("variable")
                    .str.split_exact(by="_", n=2)
                    .struct.rename_fields(["table_id", "column_id", "line_id"])
                    .alias("parts")
                )
                .with_columns(table_type_expr)
                .unnest("parts")
                .with_columns(
                    pl.col("column_id")
                    .str.slice(-2)
                    .str.to_integer()
                    .alias("column_number"),
                )
                .with_columns(common_var_meta_expr)
                .with_columns(common_line_expr)
                .with_columns(pl.col(pl.String).replace("", None))
            )

        if self.endpoint.table_type.value == "detailed":
            split_vars = (
                origin.with_columns(
                    pl.col("variable")
                    .str.split_exact(by="_", n=1)
                    .struct.rename_fields(["table_id", "line_id"])
                    .alias("parts")
                )
                .with_columns(table_type_expr)
                .unnest("parts")
                .with_columns(common_var_meta_expr)
                .with_columns(common_line_expr)
                .with_columns(
                    pl.lit(None).cast(pl.Int64).alias("column_number"),
                    pl.lit(None).cast(pl.String).alias("column_id"),
                    pl.col(pl.String).replace("", None),
                )
            )

        if self.endpoint.table_type.value in ["unknown", "cprofile"]:
            # TODO flesh this out
            split_vars = _ensure_column_exists(origin, final_vars, "")

        extras = _ensure_column_exists(self._extra, final_vars, "")

        # add extras back and enforce column order
        split_vars = split_vars.select(final_vars)

        output = (
            (
                pl.concat([split_vars, extras], how="vertical_relaxed")
                .with_columns(pl.col(pl.String).replace("", None))
                .sort("row_id")
            )
            .with_columns(
                # TODO push this up higher and make how = "vertical"
                pl.col("line_number").str.to_integer().alias("line_number"),
                pl.col("column_number").str.to_integer().alias("column_number"),
            )
            .sort("row_id")
        )

        return output

    @computed_field
    @property
    def _extra(self) -> pl.LazyFrame:
        """
        Returns the extra, often metadata or
        geography-related rows from the LazyFrame.
        """
        return self._lazyframe.filter(
            (~pl.col("variable").str.starts_with(pl.col("group")))
            | (pl.col("group").is_null())
        )

    @computed_field
    @property
    def _no_extra(self) -> pl.LazyFrame:
        """
        Returns the LazyFrame sans extra rows like metadata
        or geography-related rows from the LazyFrame, preserves
        original row_ids.
        """

        return self._lazyframe.join(self._extra, on="row_id", how="anti")

    def fetch_tidyframe(self) -> pl.DataFrame:
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

        no_extras = (
            self._lazyframe.join(self._extra, on="row_id", how="anti")
            .with_columns(pl.lit(geos).alias("geography"))
            .drop("row_id")
            .with_row_index("row_id")
        )

        return no_extras

    @computed_field
    @cached_property
    def _lazyframe(self) -> pl.LazyFrame:
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
            .with_columns(
                pl.col("vars").str.split("_").list.first().alias("group"),
            )
        )

        relevant_variable_labels = self._var_labels.join(
            endpoint_vars, how="inner", on="group"
        )

        final_cols = [
            "variable",
            "group",
            "value",
            "label",
            "concept",
            "universe",
            "date_pulled",
        ]

        # all else fails return raw data
        raw = self._raw

        if len(raw) == 2:
            output = (
                pl.LazyFrame({"variable": raw[0], "value": raw[1]})
                .with_columns(date_pulled=dt.now())
                .join(relevant_variable_labels, how="left", on="variable")
                .select(final_cols)
            ).with_row_index("row_id")

        if len(raw) > 2:
            all_frames = []
            variables = raw[0]

            for value in raw[1:]:
                lf = (
                    pl.LazyFrame({"variable": variables, "value": value})
                    .with_columns(date_pulled=dt.now())
                    .join(relevant_variable_labels, how="left", on="variable")
                    .select(final_cols)
                )

            all_frames.append(lf)

            output = pl.concat(all_frames).with_row_index("row_id")

        return output

    @computed_field
    @cached_property
    def _var_labels(self) -> pl.LazyFrame:
        """
        Fetches the human-readable variable labels
        as a list and caches it.
        """

        raw = _get(self.endpoint.variable_endpoint, self.endpoint.dataset)

        final_vars = ["variable", "label", "concept", "group", "universe"]
        default_value = "unknown as queried"

        # Cherry-picked variables pull down the entire
        # variable catalog as a list with less meta info
        if isinstance(raw, list):
            output = (
                pl.from_dicts(raw)
                .transpose(column_names="column_0")
                .lazy()
                .with_columns(
                    pl.col("name").alias("variable"),
                    pl.col("name").str.split("_").list.first().alias("group"),
                    pl.lit(default_value).alias("universe"),
                )
                .select(final_vars)
            )

        # targeted variable endpoint yield more meta info
        # and come back as a dictionary
        if isinstance(raw, dict):
            output = (
                pl.from_dicts(raw.get("variables"))
                .with_row_index(name="index")
                .unpivot(index="index")
                .lazy()
                .with_columns(pl.col("value").struct.unnest())
            )

            output = _ensure_column_exists(output, final_vars, default_value)
            output = output.select(final_vars)

        return output

    @computed_field
    @cached_property
    def _raw(self) -> list[str]:
        """
        Fetches the raw data from the API and returns
        it as a list and caches it.
        """
        return _get(self.endpoint.full_url, self.endpoint.dataset)

    def __repr__(self):
        return (
            f"APIData(\n\tendpoint='{self.endpoint.url_no_key}',\n"
            f"\tconcept/s='{self.concept}'"
        )
