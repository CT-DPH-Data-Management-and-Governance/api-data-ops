import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md("""# Adhoc StarModel Builder""")
    return


@app.cell
def _(mo):
    mo.md("""## Read-in Parquet""")
    return


@app.cell
def _(mo):
    data_file = mo.ui.file(filetypes=[".parquet"])
    data_file
    return (data_file,)


@app.cell
def _(mo):
    mo.md("""Parquet Data Preview:""")
    return


@app.cell
def _(data_file, pl):
    if data_file.value:
        long_data = pl.scan_parquet(data_file.contents())
    else:
        long_data = pl.LazyFrame()


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
    star = False
    try:
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

    except Exception as e:
        print(f"An error occurred: {e}")
    return (star,)


@app.cell
def _(star):
    if star:
        fact = star.fact
        dim_health_indicator = star.dim_health_indicator
        dim_concept = star.dim_concept
        dim_dataset = star.dim_dataset
        dim_endpoint = star.dim_endpoint
        dim_stratifiers = star.dim_stratifiers
        dim_universe = star.dim_universe
        dim_valuetype = star.dim_valuetype
    return (
        dim_concept,
        dim_dataset,
        dim_endpoint,
        dim_health_indicator,
        dim_stratifiers,
        dim_universe,
        dim_valuetype,
        fact,
    )


@app.cell
def _(mo):
    mo.md("""Fact:""")
    return


@app.cell
def _(fact, pl, star):
    if star:
        _head = fact.head(15).collect()
    else:
        _head = pl.DataFrame().head()
    _head
    return


@app.cell
def _(mo):
    parquet_export_button = mo.ui.run_button(
        tooltip="Export Model to parquet files.",
        label="Export Model",
    )
    parquet_export_button
    return (parquet_export_button,)


@app.cell
def _(
    dim_concept,
    dim_dataset,
    dim_endpoint,
    dim_health_indicator,
    dim_stratifiers,
    dim_universe,
    dim_valuetype,
    fact,
    mo,
    parquet_export_button,
):
    parquet_export_button
    with mo.status.spinner(
        title="Exporting to Parquet...", remove_on_exit=False
    ) as _spinner:
        _spinner.update("Writing Fact Parquet...")

        fact.sink_parquet("fact.parquet")

        _spinner.update("Writing out Dimensions...")
        dim_health_indicator.sink_parquet("dim_health_indicator.parquet")
        dim_concept.sink_parquet("dim_concept.parquet")
        dim_dataset.sink_parquet("dim_dataset.parquet")
        dim_endpoint.sink_parquet("dim_endpoint.parquet")
        dim_stratifiers.sink_parquet("dim_stratifiers.parquet")
        dim_universe.sink_parquet("dim_universe.parquet")
        dim_valuetype.sink_parquet("dim_valuetype.parquet")

        _spinner.update("Done - Parquet files saved.")
    return


@app.cell
def _():
    import marimo as mo
    import polars as pl

    from dataops.builders.starmodel import ACSStarModelBuilder
    return ACSStarModelBuilder, mo, pl


if __name__ == "__main__":
    app.run()
