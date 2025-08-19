import polars as pl
import polars.selectors as cs
from datetime import datetime as dt

from dataops.apis.acs import APIData
from functools import cached_property
from pydantic import (
    BaseModel,
    computed_field,
)
from pydantic_settings import SettingsConfigDict


class ACSStarModel(BaseModel):
    fact: pl.LazyFrame
    dim_stratifiers: pl.LazyFrame
    dim_universe: pl.LazyFrame
    dim_concept: pl.LazyFrame
    dim_valuetype: pl.LazyFrame
    dim_measure: pl.LazyFrame
    dim_endpoint: pl.LazyFrame
    dim_dataset: pl.LazyFrame

    model_config = SettingsConfigDict(arbitrary_types_allowed=True)


class ACSStarModelBuilder(BaseModel):
    api_data: APIData | pl.LazyFrame
    fact: pl.LazyFrame = pl.LazyFrame()
    dim_measure: pl.LazyFrame = pl.LazyFrame()
    dim_stratifiers: pl.LazyFrame = pl.LazyFrame()
    dim_universe: pl.LazyFrame = pl.LazyFrame()
    dim_concept: pl.LazyFrame = pl.LazyFrame()
    dim_endpoint: pl.LazyFrame = pl.LazyFrame()
    dim_valuetype: pl.LazyFrame = pl.LazyFrame()
    dim_dataset: pl.LazyFrame = pl.LazyFrame()

    model_config = SettingsConfigDict(arbitrary_types_allowed=True)

    @computed_field
    @cached_property
    def _starter(self) -> pl.LazyFrame:
        if isinstance(self.api_data, APIData):
            user_input = self.api_data.long()
        else:
            user_input = self.api_data

        starter = user_input.with_columns(
            pl.lit(None).cast(pl.UInt32).alias("DimUniverseID"),
            pl.lit(None).cast(pl.UInt32).alias("DimConceptID"),
            pl.lit(None).cast(pl.UInt32).alias("DimEndpointID"),
            pl.lit(None).cast(pl.UInt32).alias("DimDatasetID"),
            pl.lit(None).cast(pl.UInt32).alias("DimValueTypeID"),
            pl.lit(None).cast(pl.UInt32).alias("DimMeasureID"),
        ).with_columns(
            pl.struct(["endpoint", "stratifier_id"])
            .rank("dense")
            .alias("endpoint_based_strat_id"),
            pl.when(pl.col("measure_id").is_not_null())
            .then(
                pl.struct(
                    pl.col("universe").rank("dense").alias("DimUniverseID"),
                    pl.col("concept").rank("dense").alias("DimConceptID"),
                    pl.col("endpoint").rank("dense").alias("DimEndpointID"),
                    pl.col("dataset").rank("dense").alias("DimDatasetID"),
                    pl.col("value_type").rank("dense").alias("DimValueTypeID"),
                    pl.struct(["endpoint", "measure_id"])
                    .rank("dense")
                    .alias("DimMeasureID"),
                )
            )
            .otherwise(
                pl.struct(
                    "DimUniverseID",
                    "DimConceptID",
                    "DimEndpointID",
                    "DimDatasetID",
                    "DimValueTypeID",
                    "DimMeasureID",
                )
            )
            .struct.unnest(),
        )

        return starter

    @computed_field
    @property
    def _strats(self) -> pl.LazyFrame:
        """Return the stratifier data from the APIData"""

        return self._starter.filter(pl.col("measure_id").is_null()).collect().lazy()

    @computed_field
    @property
    def _long(self) -> pl.LazyFrame:
        """Return the long data from the APIData"""

        return self._starter.filter(pl.col("measure_id").is_not_null()).collect().lazy()

    def set_fact(self, fact: pl.DataFrame | None = None) -> "ACSStarModelBuilder":
        if fact is not None:
            self.fact = fact.lazy()
            return self

        now = dt.now().strftime("%Y-%m-%d %H:%M:%S")

        fact = (
            self._long.drop(
                [
                    "row_id",
                    "universe",
                    "concept",
                    "endpoint",
                    "dataset",
                    "value_type",
                    "stratifier_id",
                    "endpoint_based_strat_id",
                    "measure_id",
                    "measure",
                ]
            )
            .with_row_index(name="FactACSID", offset=1)
            .select(
                pl.col("FactACSID"),
                pl.col("value").alias("value_text"),
                pl.col("value").cast(pl.Float64, strict=False).alias("value_numeric"),
                pl.col("year"),
                cs.starts_with("Dim"),
                pl.col("date_pulled"),
                pl.lit(now).alias("CreatedOn"),
                pl.lit(now).alias("ModifiedOn"),
            )
        )

        self.fact = fact
        return self

    def set_measure(self, measure: pl.DataFrame | None = None) -> "ACSStarModelBuilder":
        if measure is not None:
            self.dim_measure = measure.lazy()
            return self

        measure = (
            self._long.select(["DimMeasureID", "measure"])
            .unique()
            .sort(by="DimMeasureID")
        )

        self.dim_measure = measure
        return self

    def set_universe(
        self, universe: pl.DataFrame | None = None
    ) -> "ACSStarModelBuilder":
        if universe is not None:
            self.dim_universe = universe.lazy()
            return self

        universe = (
            self._long.select(["DimUniverseID", "universe"])
            .unique()
            .sort(by="DimUniverseID")
        )

        self.dim_universe = universe
        return self

    def set_concept(self, concept: pl.DataFrame | None = None) -> "ACSStarModelBuilder":
        if concept is not None:
            self.dim_concept = concept.lazy()
            return self

        concept = (
            self._long.select(["DimConceptID", "concept"])
            .unique()
            .sort(by="DimConceptID")
        )

        self.dim_concept = concept
        return self

    def set_endpoint(
        self, endpoint: pl.DataFrame | None = None
    ) -> "ACSStarModelBuilder":
        if endpoint is not None:
            self.dim_endpoint = endpoint.lazy()
            return self

        endpoint = (
            self._long.select(["DimEndpointID", "endpoint"])
            .unique()
            .sort(by="DimEndpointID")
        )

        self.dim_endpoint = endpoint
        return self

    def set_valuetype(
        self, valuetype: pl.DataFrame | None = None
    ) -> "ACSStarModelBuilder":
        if valuetype is not None:
            self.dim_valuetype = valuetype.lazy()
            return self

        valuetype = (
            self._long.select(["DimValueTypeID", "value_type"])
            .unique()
            .sort(by="DimValueTypeID")
        )

        self.dim_valuetype = valuetype
        return self

    def set_dataset(self, dataset: pl.DataFrame | None = None) -> "ACSStarModelBuilder":
        if dataset is not None:
            self.dim_dataset = dataset.lazy()
            return self

        dataset = (
            self._long.select(["DimDatasetID", "dataset"])
            .unique()
            .sort(by="DimDatasetID")
        )

        self.dim_dataset = dataset
        return self

    def set_stratifiers(
        self, stratifiers: pl.DataFrame | None = None
    ) -> "ACSStarModelBuilder":
        if stratifiers is not None:
            self.dim_stratifiers = stratifiers.lazy()
            return self

        var_values = self._strats.select(
            ["value", "variable", "endpoint_based_strat_id"]
        )

        variable_sets = (
            var_values.select(["variable", "endpoint_based_strat_id"])
            .sort(["endpoint_based_strat_id", "variable"])
            .group_by("endpoint_based_strat_id", maintain_order=True)
            .agg(pl.col("variable").unique(maintain_order=True).alias("variable_set"))
            .with_columns(
                pl.struct("variable_set").rank("dense").alias("variable_set_id")
            )
        )

        var_values_wids = var_values.join(
            variable_sets, on="endpoint_based_strat_id", how="left"
        )

        dim_starter = (
            var_values_wids.select(
                ["variable_set_id", "endpoint_based_strat_id", "variable", "value"]
            )
            .collect()
            .pivot(
                on="variable",
                index=["variable_set_id", "endpoint_based_strat_id"],
                values="value",
            )
            .lazy()
            .with_columns(
                pl.struct(pl.exclude("endpoint_based_strat_id"))
                .rank("dense")
                .alias("var_set_value_id")
            )
        )

        dim_walk = (
            dim_starter.select(
                pl.col("var_set_value_id").alias("DimStratifierID"),
                pl.exclude(["variable_set_id", "var_set_value_id"]),
            )
            .unpivot(index=["DimStratifierID", "endpoint_based_strat_id"])
            .sort("DimStratifierID", "endpoint_based_strat_id")
        )

        dim_stratifier = (
            dim_walk.drop("endpoint_based_strat_id")
            .unique()
            .sort("DimStratifierID")
            .rename({"variable": "stratifier_variable", "value": "stratifier_value"})
        )

        dim_crosswalk = dim_walk.select(
            ["endpoint_based_strat_id", "DimStratifierID"]
        ).unique()

        new_starter = self._starter.join(
            dim_crosswalk, on="endpoint_based_strat_id", how="left"
        )

        self.dim_stratifiers = dim_stratifier
        self._starter = new_starter

        return self

    def build(self) -> ACSStarModel:
        """Builds and returns the final model"""
        return ACSStarModel(
            fact=self.fact,
            dim_measure=self.dim_measure,
            dim_stratifiers=self.dim_stratifiers,
            dim_universe=self.dim_universe,
            dim_concept=self.dim_concept,
            dim_endpoint=self.dim_endpoint,
            dim_dataset=self.dim_dataset,
            dim_valuetype=self.dim_valuetype,
        )
