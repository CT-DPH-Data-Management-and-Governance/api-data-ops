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
    # dim measure
    dim_endpoint: pl.LazyFrame
    # dim year
    dim_dataset: pl.LazyFrame

    model_config = SettingsConfigDict(arbitrary_types_allowed=True)


class ACSStarModelBuilder(BaseModel):
    api_data: APIData | pl.LazyFrame  # add validations to check if APIData.long()
    fact: pl.LazyFrame = pl.LazyFrame()
    dim_stratifiers: pl.LazyFrame = pl.LazyFrame()
    dim_universe: pl.LazyFrame = pl.LazyFrame()
    dim_concept: pl.LazyFrame = pl.LazyFrame()
    dim_endpoint: pl.LazyFrame = pl.LazyFrame()
    dim_valuetype: pl.LazyFrame = pl.LazyFrame()
    dim_dataset: pl.LazyFrame = pl.LazyFrame()

    model_config = SettingsConfigDict(arbitrary_types_allowed=True)

    @computed_field
    @cached_property
    def _long(self) -> pl.LazyFrame:
        """Return the long data from the APIData"""
        return (
            self.api_data.long()
            .with_columns(
                pl.col("universe").rank("dense").alias("DimUniverseID"),
                pl.col("concept").rank("dense").alias("DimConceptID"),
                pl.col("endpoint").rank("dense").alias("DimEndpointID"),
                pl.col("year").rank("dense").alias("DimYearID"),
                pl.col("dataset").rank("dense").alias("DimDatasetID"),
                pl.col("value_type").rank("dense").alias("DimValueTypeID"),
            )
            .collect()
            .lazy()
        )
        # create IDs for each one of the dim domains
        # int ids that are premade will need to be  "grouped by" a composite id
        # universe, concept and other strings are likely cross cutting by design
        # so the unique values of the var itself should be good

    def set_fact(self, fact: pl.DataFrame | None = None) -> "ACSStarModelBuilder":
        if fact is not None:
            self.fact = fact
            return self

        fact = self._long.drop(
            ["universe", "concept", "endpoint", "year", "dataset", "value_type"]
        )
        self.fact = fact
        return self

    def set_universe(
        self, universe: pl.DataFrame | None = None
    ) -> "ACSStarModelBuilder":
        if universe is not None:
            self.dim_universe = universe
            return self

        universe = self._long.select(["DimUniverseID", "universe"]).unique()

        self.dim_universe = universe
        return self

    def set_concept(self, concept: pl.DataFrame | None = None) -> "ACSStarModelBuilder":
        if concept is not None:
            self.dim_concept = concept
            return self

        concept = self._long.select(["DimConceptID", "concept"]).unique()

        self.dim_concept = concept
        return self

    def set_endpoint(
        self, endpoint: pl.DataFrame | None = None
    ) -> "ACSStarModelBuilder":
        if endpoint is not None:
            self.dim_endpoint = endpoint
            return self

        endpoint = self._long.select(["DimEndpointID", "endpoint"]).unique()

        self.dim_endpoint = endpoint
        return self

    def set_stratifiers(
        self, stratifiers: pl.DataFrame | None = None
    ) -> "ACSStarModelBuilder":
        if stratifiers is not None:
            self.dim_stratifiers = stratifiers
            return self

        dim = (
            self._long.filter(pl.col("measure_id").is_null())
            .select(
                pl.col("stratifier_id").alias("DimStratifierID"),
                pl.col("variable"),
                pl.col("value"),
            )
            .unique()
            .collect()
            .pivot(on="variable", index="DimStratifierID", values="value")
            .lazy()
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
            dim_stratifiers=self.dim_stratifiers,
            dim_universe=self.dim_universe,
            dim_concept=self.dim_concept,
            dim_endpoint=self.dim_endpoint,
        )
