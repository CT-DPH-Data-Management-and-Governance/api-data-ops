import polars as pl

from dataops.apis.acs import APIData, APIEndpoint
from typing import Annotated, List
from functools import cached_property
from pydantic import (
    BaseModel,
    computed_field,
    Field,
    HttpUrl,
    SecretStr,
    ValidationError,
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

        starter = (
            # need universal ids to join stuff back up
            # or couch things in when then otherwise type of stuff
            user_input.with_columns(
                pl.lit(None).cast(pl.UInt32).alias("DimUniverseID"),
                pl.lit(None).cast(pl.UInt32).alias("DimConceptID"),
                pl.lit(None).cast(pl.UInt32).alias("DimEndpointID"),
                pl.lit(None).cast(pl.UInt32).alias("DimDatasetID"),
                pl.lit(None).cast(pl.UInt32).alias("DimValueTypeID"),
                pl.lit(None).cast(pl.UInt32).alias("DimMeasureID"),
            )
            .with_columns(
                pl.struct(["endpoint", "stratifier_id"])
                .rank("dense")
                .alias("endpoint_based_strat_id"),
                pl.when(pl.col("measure_id").is_not_null()).then(
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
                ),
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
                .struct.unnest(),
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

        fact = self._long.drop(
            ["row_id", "universe", "concept", "endpoint", "dataset", "value_type"]
        ).rename({"stratifier_id": "DimStratifierID"})
        self.fact = fact
        return self

    def set_measure(self, measure: pl.DataFrame | None = None) -> "ACSStarModelBuilder":
        if measure is not None:
            self.dim_measure = measure.lazy()
            return self

        measure = (
            self._long.select(["DimMeasureID", "Measure"])
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

    def set_stratifiers_wide(
        self, stratifiers: pl.DataFrame | None = None
    ) -> "ACSStarModelBuilder":
        if stratifiers is not None:
            self.dim_stratifiers = stratifiers.lazy()
            return self

        dim = (
            self._strats.select(
                pl.col("stratifier_id").alias("DimStratifierID"),
                pl.col("variable"),
                pl.col("value"),
            )
            .unique()
            .collect()
            .pivot(on="variable", index="DimStratifierID", values="value")
            .sort(by="DimStratifierID")
            .lazy()
        )

        self.dim_stratifiers = dim
        return self

    def set_stratifiers(
        self, stratifiers: pl.DataFrame | None = None
    ) -> "ACSStarModelBuilder":
        if stratifiers is not None:
            self.dim_stratifiers = stratifiers.lazy()
            return self

        # go wide, create IDS, then go long
        # join back to a starter frame, and then overwrite starter with it
        # return both with self

        # doesnt work

        # endpoint_based_start_id works almost, there are some doubled up values
        # so we need endpoint_bnased_start_id to variablke and value combe to dim id

        dim_and_crosswalk = (
            # self._strats.select(
            strats.select(
                pl.col("variable").alias("stratifier_type"),
                pl.col("value").alias("stratifier_value"),
                pl.col("endpoint_based_strat_id").alias("id"),
            )
            .unique()
            .sort("id")
            .with_columns(
                pl.struct("stratifier_type", "stratifier_value")
                .rank("dense")
                .alias("DimStratifierID")
            )
        )

        # forget it - just join type and value to bring id along for the ride
        # or maybe struct (type and value to create an id and bring it along?)
        take_a_look = (
            dim_and_crosswalk.drop("DimStratifierID")
            .unique(["stratifier_type", "stratifier_value"], keep="first")
            .sort("id")
        )

        # weirdly it looks like the original stratifier id works... but
        # that seems like a bug, it should restart at 0 for each dataset
        # so there should be more overlap.
        # maybe a lazy optimization thing.
        # so I need to probably just ignore all those ids
        # go based on values. and then do a new measure id here, if I can?

        # starter.join(take_a_look, left_on=["variable", "value"], right_on=["stratifier_type", "stratifier_value"])

        dim = (
            dim_and_crosswalk.select(
                ["DimStratifierID", "stratifier_type", "stratifier_value"]
            )
            .unique()
            .sort("DimStratifierID")
        )

        dim_and_crosswalk = dim_and_crosswalk.select(["id", "DimStratifierID"])

        # ack doesnt quite work

        # I just need to ONLY  dim the variable and value combos
        # join em back to the dataset by rowid
        # then worry about propagating to each measure

        # what if we used the now unique stratifier_ids with the rows, this would
        # uniquely identify each of the measure and geo combos. it just becomes
        # the new rowid still. doesnt work.

        # is it just the endpoint based start id?
        # ^ no because we want a measure with the same exact set of vars to have
        # the same dim id

        # new_self = (
        #     self._starter.join(dim_and_crosswalk,)
        # )

        self.dim_stratifiers = dim
        return self

    def set_stratifiers_long(
        self, stratifiers: pl.DataFrame | None = None
    ) -> "ACSStarModelBuilder":
        if stratifiers is not None:
            self.dim_stratifiers = stratifiers.lazy()
            return self

        measure_to_strat = (
            self._starter.select(["measure_id", "endpoint_based_strat_id"])
            .drop_nulls()
            .unique()
        )

        dim = (
            self._strats.with_columns()
            .select(
                pl.col("stratifier_id").alias("DimStratifierID"),
                pl.col("variable").alias("stratifier_type"),
                pl.col("value").alias("stratifier_value"),
            )
            .unique()
            .sort(by="DimStratifierID")
        )

        self.dim_stratifiers = dim
        return self

    # TODO set_stnd_stratifiers so like take NAME if it exists otherwise use an algo

    # def set_acs_data(self, acs_data: APIData) -> "ACSStarModelBuilder":
    #     self.data["acs_data"] = acs_data
    #     return self

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
