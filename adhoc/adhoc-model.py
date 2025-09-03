import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md("""# Adhoc StarModel Builder""")
    return


@app.cell
def _(mo):
    mo.md("""## Data Read""")
    return


@app.cell
def _(pl):
    long_data = pl.scan_parquet("adhoc/adhoc.parquet")
    long_data.head().collect()
    return (long_data,)


@app.cell
def _(mo):
    mo.md("""## Star Model Builder""")
    return


@app.cell
def _(ACSStarModelBuilder, long_data):
    builder = ACSStarModelBuilder(api_data=long_data)
    return (builder,)


@app.cell
def _(builder):
    star = (
        builder.set_stratifiers()
        .set_concept()
        .set_endpoint()
        .set_valuetype()
        .set_dataset()
        .set_universe()
        .set_health_indicator()
        .set_fact()
        .build()
    )
    return (star,)


@app.cell
def _(star):
    star.fact.collect()
    return


@app.cell
def _(star):
    star.dim_health_indicator.sort("health_indicator").collect()
    return


@app.cell
def _(star):
    star.dim_concept.collect()
    return


@app.cell
def _(star):
    star.dim_dataset.collect()
    return


@app.cell
def _(star):
    star.dim_endpoint.collect()
    return


@app.cell
def _(star):
    star.dim_stratifiers.collect()
    return


@app.cell
def _(star):
    star.dim_universe.collect()
    return


@app.cell
def _(star):
    star.dim_valuetype.collect()
    return


@app.cell
def _(star):
    huh = (
        star.fact.group_by("DimHealthIndicatorID")
        .len()
        .sort(["len", "DimHealthIndicatorID"], descending=True)
        .head()
        .select("DimHealthIndicatorID")
        .collect()
        .to_series()
        .to_list()
    )
    huh
    return (huh,)


@app.cell
def _(huh, pl, star):
    star.dim_health_indicator.filter(
        pl.col("DimHealthIndicatorID").is_in(huh)
    ).collect()
    return


@app.cell
def _(huh, pl, star):
    star.fact.filter(pl.col("DimHealthIndicatorID").is_in(huh)).collect()
    return


@app.cell
def _(pl, star):
    star.fact.filter(pl.col("DimHealthIndicatorID").eq(813)).filter(
        pl.col("DimValueTypeID").eq(5)
    ).collect()
    return


@app.cell
def _(mo):
    mo.md(
        "some of this stuff looks weird - but it might be better to let others take a look as well."
    )
    return


@app.cell
def _(mo):
    mo.md("TODO: add a dim_acs_variable etc...")
    return


@app.cell
def _(mo):
    mo.md("## write it out")
    return


@app.cell
def _(star):
    star
    return


@app.cell(disabled=True)
def _(star):
    # TODO: add this as a method to builder
    begin = "adhoc/adhoc_"
    end = ".parquet"
    star.fact.sink_parquet(f"{begin}fact{end}")
    star.dim_stratifiers.sink_parquet(f"{begin}stratifiers{end}")
    star.dim_universe.sink_parquet(f"{begin}universe{end}")
    star.dim_concept.sink_parquet(f"{begin}concept{end}")
    star.dim_valuetype.sink_parquet(f"{begin}valuetype{end}")
    star.dim_health_indicator.sink_parquet(f"{begin}healthindicator{end}")
    star.dim_endpoint.sink_parquet(f"{begin}endpoint{end}")
    star.dim_dataset.sink_parquet(f"{begin}dataset{end}")
    return


@app.cell
def _():
    import marimo as mo
    import polars as pl

    from dataops.builders.starmodel import ACSStarModelBuilder
    return ACSStarModelBuilder, mo, pl


if __name__ == "__main__":
    app.run()
