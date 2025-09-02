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

from dataops.helpers.polars import ensure_column_exists
from dataops.http.data import get
from dataops.settings.acs import AppSettings


class TableType(str, Enum):
    subject = "subject"
    detailed = "detailed"
    cprofile = "cprofile"
    dataprofile = "dataprofile"
    unknown = "unknown"


class APIEndpointMixin:
    """A mixin to add methods to APIEndpoint."""

    @model_validator(mode="before")
    @classmethod
    def set_api_key_from_env(cls, data: Any) -> Any:
        """Sets API key from env var if not provided."""
        if isinstance(data, dict) and not data.get("api_key"):
            data["api_key"] = AppSettings().census.token.get_secret_value()
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
            case TableType.subject:
                if self.group is None:
                    return f"{self.base_url}/{self.year}/{self.dataset}/variables"
                else:
                    return f"{self.base_url}/{self.year}/{self.dataset}/groups/{self.group}"
            case _:
                return f"{self.base_url}/{self.year}/{self.dataset}/groups/{self.group}"

    @computed_field
    @property
    def group(self) -> str:
        _variable_string = "".join(self.variables)
        _length = len(self.variables)
        _starts_with = _variable_string.startswith("group")
        _is_group = (_length < 2) and (_starts_with)

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

        _is_group = (_length < 2) and (_starts_with) and (_maybe_detailed)

        tabletype = TableType.unknown

        if _is_group:
            tabletype = TableType.detailed
            return tabletype

        elif last == "profile":
            profile_type = self.group[0]
            match profile_type:
                case "D":
                    tabletype = TableType.dataprofile
                    return tabletype
                case "C":
                    tabletype = TableType.cprofile
                    return tabletype
                case _:
                    tabletype = TableType.unknown
                    return tabletype

        else:
            try:
                tabletype = TableType[last]
            except KeyError:
                tabletype = TableType.unknown
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
        variable_groups = (
            pl.LazyFrame({"variable": self.endpoint.variables})
            .with_columns(
                pl.col("variable")
                .str.replace_all("\\(|\\)", " ")
                .str.strip_chars()
                .str.split(by=" ")
            )
            .select("variable")
            .collect()
            .explode("variable")
            .select(
                pl.col("variable").str.split("_").list.first().alias("computed_group"),
            )
            .unique()
            .to_series()
            .to_list()
        )

        relevant_variable_labels = self._var_labels.filter(
            pl.col("group").is_in(variable_groups)
        )

        final_cols = [
            "row_id",
            "stratifier_id",
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

        # start wide to accomadate any kind of endpoint
        # add a stratifier id to accomadate any kind of strat/geo etc...
        # then unpivot to longer for easy joins.

        output = (
            pl.LazyFrame(data=raw[1:], schema=raw[0], orient="row")
            .with_row_index(name="stratifier_id", offset=1)
            .unpivot(
                index="stratifier_id", value_name="value", variable_name="variable"
            )
            .join(relevant_variable_labels, how="left", on="variable")
            .with_row_index("row_id", offset=1)
            .with_columns(date_pulled=dt.now())
            .select(final_cols)
        )

        return output.collect().lazy()

    @computed_field
    @property
    def _rawframe_long(self) -> pl.LazyFrame:
        """
        Returns the API Response data back in a dataframe
        closely mirroring the raw response text, often as a
        monstrously wide table.
        """

        return (
            pl.LazyFrame(data=self._raw[1:], schema=self._raw[0], orient="row")
            .with_row_index("stratifier_id")
            .unpivot(
                index="stratifier_id", value_name="value", variable_name="variable"
            )
        )

    @computed_field
    @property
    def _rawframe_wide(self) -> pl.LazyFrame:
        """
        Returns the API Response data back in a dataframe
        closely mirroring the raw response text, often as a
        monstrously wide table.
        """

        return pl.LazyFrame(data=self._raw[1:], schema=self._raw[0], orient="row")

    @computed_field
    @property
    def _extra(self) -> pl.LazyFrame:
        """
        Returns the extra, often metadata or
        geography-related rows from the LazyFrame.
        """

        return self._lazyframe.with_columns(
            pl.col("variable").str.split("_").list.first().alias("computed_group"),
            pl.col("group").unique().drop_nulls().implode().alias("expected_groups"),
        ).filter(~pl.col("computed_group").is_in(pl.col("expected_groups")))

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

        measure_id_expr = (
            pl.struct("stratifier_id", "column_number", "line_number")
            .rank("dense")
            .alias("measure_id")
        )

        # some of the tables don't fully adhere to be able to use slicing
        common_line_expr = (
            pl.col("line_id")
            .str.extract(r"^(\d+)")
            .alias("line_number")
            .str.to_integer(),
            pl.col("line_id").str.extract(r"(\D+)$").alias("line_suffix"),
            # pl.col("line_id").str.slice(0, 3).alias("line_number").str.to_integer(),
            # pl.col("line_id").str.slice(3).alias("line_suffix"),
        )

        final_vars = [
            "stratifier_id",
            "row_id",
            "variable",
            "measure_id",
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
                .with_columns(measure_id_expr)
            )

        if self.endpoint.table_type.value in ["detailed", "dataprofile"]:
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
                .with_columns(measure_id_expr)
            )

        if self.endpoint.table_type.value in ["unknown", "cprofile"]:
            # TODO flesh this out
            split_vars = ensure_column_exists(origin, final_vars, "")

        extras = ensure_column_exists(self._extra, final_vars, "").select(final_vars)

        # add extras back and enforce column order
        split_vars = split_vars.select(final_vars)

        output = (
            (
                pl.concat([split_vars, extras], how="vertical_relaxed")
                .with_columns(pl.col(pl.String).replace("", None))
                .sort("row_id")
            )
            .with_columns(
                pl.col("line_number").str.to_integer(strict=False).alias("line_number"),
                pl.col("column_number")
                .str.to_integer(strict=False)
                .alias("column_number"),
                pl.col("measure_id").str.to_integer(strict=False).alias("measure_id"),
            )
            .sort(["row_id", "stratifier_id"])
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
            "label_end_misc",
        ]

        output = (
            origin.with_columns(
                pl.col("label")
                .str.count_matches("!!", literal=True)
                .alias("exclaim_count"),
                pl.col("label")
                .str.split_exact("!!", 4)
                .struct.rename_fields(common_label_parts)
                .alias("parts"),
            )
            .unnest("parts")
            .with_columns(pl.col("label_end_misc").fill_null(""))
            .with_columns(
                pl.col(common_label_parts)
                .str.replace_all(r"--|:", "")
                .str.strip_chars()
                .str.to_lowercase()
            )
            .with_columns(
                pl.concat_str(["label_end", "label_end_misc"], separator=" ").alias(
                    "label_end"
                )
            )
            .drop("label_end_misc")
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

        raw = get(self.endpoint.variable_endpoint, self.endpoint.dataset)

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

            output = ensure_column_exists(output, final_vars, default_value)
            output = output.select(final_vars)

        return output.collect().lazy()

    @computed_field
    @cached_property
    def _raw(self) -> list[str]:
        """
        Fetches the raw data from the API and returns
        it as a list and caches it.
        """
        return get(self.endpoint.full_url, self.endpoint.dataset)
