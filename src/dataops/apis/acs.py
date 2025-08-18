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

from dataops.mixins.acs import APIDataMixin, APIEndpointMixin, APIRequestMixin


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
            description="Your Census API key. If not provided, it's sourced from the CENSUS__TOKEN environment variable.",
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

    def storage(self) -> pl.LazyFrame:
        """
        generate a long table - with more integer ids.
        halfway between wide and long. E/EA/M/MA etc... are kept
        long like long. but the _extras are kept in a single column,
        like wide, but always one column. <- or something like that?

        or mayeb do an algo - since we'll always have url geography,
        look for name, if no name then look for x, then y etc...
        so maybe 2 columns, url geos, and consolidated extras col.

        """

        # placeholder
        return self.long()

    def long(self) -> pl.LazyFrame:
        """
        generate a long table
        """

        order = [
            "stratifier_id",
            "row_id",
            "measure_id",
            "universe",
            "concept",
            "measure",
            "value_type",
            "value",
            "variable",
            "endpoint",
            "year",
            "dataset",
            "date_pulled",
        ]

        dataset = self.endpoint.dataset

        long = (
            self.standard_parse()
            .drop("row_id")
            .with_columns(pl.col("label_end").fill_null(""))
            .with_columns(
                pl.concat_str(
                    [
                        pl.col("label_concept_base"),
                        pl.lit(": "),
                        pl.col("label_stratifier"),
                        pl.lit(" - "),
                        pl.col("label_end"),
                    ]
                )
                .str.strip_chars_end(" - ")
                .str.strip_chars()
                .alias("measure"),
                pl.when(pl.col("label_line_type").is_null())
                .then(pl.col("variable"))
                .otherwise(pl.col("label_line_type"))
                .alias("value_type"),
                pl.lit(dataset).alias("dataset"),
            )
            .with_columns(
                pl.when(pl.col("universe").is_null())
                .then(pl.col("value_type"))
                .otherwise("universe")
                .alias("universe"),
                pl.when(pl.col("concept").is_null())
                .then(pl.col("value_type"))
                .otherwise("concept")
                .alias("concept"),
                pl.when(pl.col("measure").is_null())
                .then(pl.col("value_type"))
                .otherwise("measure")
                .alias("measure"),
            )
            .sort(["stratifier_id", "variable"])
            .with_row_index("row_id", offet=1)
            .select(order)
            .collect()
            .lazy()
        )

        return long

    def wide(self) -> pl.LazyFrame:
        """
        generate a wide table

        """

        static_stratifier_cols = (
            self._extra.select("variable").unique().collect().to_series().to_list()
        )

        static_stratifier_values = (
            self._extra.select(["stratifier_id", "variable", "value"])
            .unique()
            .with_columns(
                pl.col("variable").str.to_lowercase().str.replace_all(" ", "_")
            )
            .collect()
            .pivot("variable", index="stratifier_id", values="value")
            .lazy()
        )

        # consider dropping id - and just join to every row at end?
        static_meta_cols = (
            self._no_extra.select(
                [
                    "stratifier_id",
                    "universe",
                    "concept",
                ]
            )
            .drop_nulls()
            .unique()
        )

        human_var_labels = (
            self.standard_parse()
            .select(
                [
                    "variable",
                    "label_concept_base",
                    "label_stratifier",
                    "label_end",
                ]
            )
            .filter(~pl.col("variable").is_in(static_stratifier_cols))
            .unique()
            .fill_null("")
            .select(
                pl.col("variable"),
                pl.concat_str(
                    [
                        pl.col("label_concept_base"),
                        pl.lit(": "),
                        pl.col("label_stratifier"),
                        pl.lit(" - "),
                        pl.col("label_end"),
                    ]
                )
                .str.strip_chars_end(" - ")
                .str.strip_chars()
                .alias("measure"),
            )
        )

        content = (
            self.standard_parse()
            .join(self._extra, on="row_id", how="anti")
            .select(["stratifier_id", "label_line_type", "variable", "value"])
            # .fill_null("")
            .join(human_var_labels, on="variable", how="left")
            .drop("variable")
            .with_columns(
                pl.struct(["stratifier_id", "measure"]).rank("dense").alias("comp_id")
            )
        )

        comp_ids = content.select(["comp_id", "stratifier_id", "measure"]).unique()

        wide = (
            content.select(["comp_id", "label_line_type", "value"])
            .collect()
            .pivot(
                "label_line_type",
                index="comp_id",
                values="value",
                aggregate_function="first",
            )
            .lazy()
        )

        wide = comp_ids.join(wide, on="comp_id").drop("comp_id")

        # literals
        endpoint = self.endpoint.url_no_key
        date_pulled = (
            self.standard_parse().select("date_pulled").head(1).collect().item()
        )
        year = self.endpoint.year
        dataset = self.endpoint.dataset

        output = (
            (
                static_stratifier_values.join(static_meta_cols, on="stratifier_id")
                .join(wide, on="stratifier_id")
                .with_columns(
                    pl.lit(endpoint).alias("endpoint"),
                    pl.lit(year).alias("year"),
                    pl.lit(dataset).alias("dataset"),
                    pl.lit(date_pulled).alias("date_pulled"),
                )
                .sort(["stratifier_id", "measure"])
                .with_row_index("row_id")
            )
            .collect()
            .lazy()
        )

        return output

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

        content = (
            self._lazyframe.with_columns(
                pl.lit(geos).alias("url_geography"),
                pl.col("value").cast(pl.Float32, strict=False).alias("value_numeric"),
                pl.lit(year).alias("year"),
                pl.lit(endpoint).alias("endpoint"),
            )
            .collect()
            .lazy()
        )

        parsed_labels = (
            self._parse_label()
            .select(
                [
                    "row_id",
                    "exclaim_count",
                    "label_line_type",
                    "label_concept_base",
                    "label_stratifier",
                    "label_end",
                ]
            )
            .collect()
            .lazy()
        )

        parsed_vars = (
            (
                self._parse_vars()
                .select(
                    [
                        "row_id",
                        "measure_id",
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
            .collect()
            .lazy()
        )

        order = [
            "stratifier_id",
            "row_id",
            "measure_id",
            "endpoint",
            "year",
            "variable",
            "group",
            "label",
            "concept",
            "universe",
            "url_geography",
            "value",
            "value_numeric",
            "exclaim_count",
            "label_line_type",
            "label_concept_base",
            "label_stratifier",
            "label_end",
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
            "date_pulled",
        ]

        output = (
            content.join(parsed_labels, how="left", on="row_id")
            .join(parsed_vars, how="left", on="row_id")
            .select(order)
            .collect()
            .lazy()
        )

        return output

    def __repr__(self):
        return (
            f"APIData(\n\tendpoint='{self.endpoint.url_no_key}',\n"
            f"\tconcept/s='{self.concept}'"
        )
