from datetime import datetime as dt
from enum import Enum
from functools import cached_property
from typing import Any

import polars as pl
import requests
from pydantic import (
    computed_field,
    field_validator,
    model_validator,
)

from dataops._helpers import _ensure_column_exists
from dataops.api import _get
from dataops.models import settings


class TableType(str, Enum):
    subject = "subject"
    detailed = "detailed"
    cprofile = "cprofile"
    unknown = "unknown"


class APIEndpointMixin:
    """A mixin to add methods to APIEndpoint."""

    @model_validator(mode="before")
    @classmethod
    def set_api_key_from_env(cls, data: Any) -> Any:
        """Sets API key from env var if not provided."""
        if isinstance(data, dict) and not data.get("api_key"):
            data["api_key"] = settings.AppSettings().census.token.get_secret_value()
        return data

    @field_validator("dataset")
    @classmethod
    def dataset_must_not_have_leading_or_trailing_slashes(cls, v: str) -> str:
        """Ensures the dataset string is clean."""
        return v.strip("/")

    # --- Computed Properties for Functionality ---
    @computed_field
    @property
    def full_url(self) -> str:
        """Constructs the complete, queryable API URL from the model's attributes."""
        get_params = ",".join(self.variables)
        url_path = f"{self.base_url}/{self.year}/{self.dataset}"
        geo_key, geo_value = self.geography.split(":", 1)
        params = {"get": get_params, geo_key: geo_value}
        if self.api_key.get_secret_value():
            params["key"] = self.api_key.get_secret_value()
        req = requests.Request("GET", url_path, params=params)
        return req.prepare().url

    @computed_field
    @property
    def url_no_key(self) -> str:
        """Constructs the complete, queryable API URL from the model's attributes."""
        get_params = ",".join(self.variables)
        url_path = f"{self.base_url}/{self.year}/{self.dataset}"
        geo_key, geo_value = self.geography.split(":", 1)
        params = {"get": get_params, geo_key: geo_value}
        req = requests.Request("GET", url_path, params=params)
        return req.prepare().url

    @computed_field
    @property
    def variable_endpoint(self) -> str:
        """Constructs the variable API URL from the full url."""

        # should be a catch-all that overpulls but a good backstop
        last_resort = f"{self.base_url}/{self.year}/{self.dataset}/variables"

        match self.table_type:
            case TableType.unknown:
                return last_resort
            case _:
                return f"{self.base_url}/{self.year}/{self.dataset}/groups/{self.group}"

    @computed_field
    @property
    def group(self) -> str:
        _variable_string = "".join(self.variables)
        _length = len(self.variables)
        _starts_with = _variable_string.startswith("group")
        _is_group = (_length < 2) & (_starts_with)

        if _is_group:
            return _variable_string.removeprefix("group(").removesuffix(")")

        else:
            return None

    @computed_field
    @property
    def table_type(self) -> str:
        dataset_parts = self.dataset.strip("/").split("/")
        last = dataset_parts[-1]
        middle = dataset_parts[1]

        # TODO refactor to use self.group
        _variable_string = "".join(self.variables)
        _length = len(self.variables)
        _starts_with = _variable_string.startswith("group")
        _maybe_detailed = last == middle

        _is_group = (_length < 2) & (_starts_with) & (_maybe_detailed)

        if _is_group:
            return TableType.detailed

        else:
            try:
                tabletype = TableType[last]
            except KeyError:
                tabletype = TableType.unknown
            finally:
                return tabletype


class APIDataMixin:
    """A mixin to add data wrangling methods to APIData."""

    # part of repr
    @computed_field
    @cached_property
    def concept(self) -> list[str]:
        """Endpoint ACS Concept as assigned by the Census"""

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

    # core lazyframe-based
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

        output = pl.LazyFrame()
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

        return output.collect().lazy()

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

    # variables
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
    def _vars_matrix(self) -> pl.LazyFrame:
        final_vars = [
            "row_id",
            "variable",
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

        return self._parse_vars().select(final_vars)

    # labels
    def _parse_label(self) -> pl.LazyFrame:
        """
        Parse the label variable and generate columns based
        on commonly included parts. NULLs fill in when
        there is no data in that type. Up to 3 '!!' delimeters
        are split and any more text is captured in the
        catch-all `label_end` column.

        This should be more than enough for B Tables - please
        note that subject tables will often have more, and the
        parts may not follow the same format because they refer
        to the nesting of tables. For detailed parsing of
        subject tables, please see _label_matrix().
        """
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

    @computed_field
    @property
    def _label_matrix(self) -> pl.LazyFrame:
        """
        Generate a polars LazyFrame with the census data labels
        split out by their '!!' delimiter into a long table
        format, regardless of how many '!!' there were in a
        label.  `row_id` is preserved from the original
        lazyframe of data so this can always be joined back.
        """

        return self._no_extra.select(
            pl.col("row_id"),
            pl.concat_str(
                [
                    pl.col("label").str.head(10),
                    pl.lit("..."),
                    pl.col("label").str.tail(10),
                ]
            ).alias("trunc_orig_label"),
            pl.col("label")
            .str.count_matches("!!", literal=True)
            .alias("exclaim_count"),
            pl.col("label").str.split("!!").alias("label_parts"),
        ).explode("label_parts")


class APIRequestMixin:
    """A mixin for adding the API request methods to APIData."""

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

        return output.collect().lazy()

    @computed_field
    @cached_property
    def _raw(self) -> list[str]:
        """
        Fetches the raw data from the API and returns
        it as a list and caches it.
        """
        return _get(self.endpoint.full_url, self.endpoint.dataset)
