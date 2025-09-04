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
def _(data_file, mo):
    has_data = False

    if data_file.value:
        has_data = True
        mo.md("""Parquet Data Preview:""")
    else:
        mo.stop(True)
    return (has_data,)


@app.cell
def _(data_file, has_data, mo, pl):
    if has_data:
        long_data = pl.scan_parquet(data_file.contents())
    else:
        mo.stop(True, mo.md("**Upload parquet data to continue.**"))

    mo.md("""## Star Model Builder""")
    return (long_data,)


@app.cell
def _(long_data):
    long_data.head(15).collect()
    return


@app.cell
def _(ACSStarModelBuilder, long_data):
    builder = ACSStarModelBuilder(api_data=long_data)
    return (builder,)


@app.cell
def _(builder):
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
    mo.md("""**Fact:**""")
    return


@app.cell
def _(fact):
    fact.head(15).collect()
    return


@app.cell
def _(has_data, mo):
    if has_data:
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
    parquet_export_button,
):
    if parquet_export_button.value:
        fact.sink_parquet("fact.parquet")

        dim_health_indicator.sink_parquet("dim_health_indicator.parquet")
        dim_concept.sink_parquet("dim_concept.parquet")
        dim_dataset.sink_parquet("dim_dataset.parquet")
        dim_endpoint.sink_parquet("dim_endpoint.parquet")
        dim_stratifiers.sink_parquet("dim_stratifiers.parquet")
        dim_universe.sink_parquet("dim_universe.parquet")
        dim_valuetype.sink_parquet("dim_valuetype.parquet")
    return


@app.cell
def _():
    import marimo as mo
    import polars as pl

    from dataops.builders.starmodel import ACSStarModelBuilder
    return ACSStarModelBuilder, mo, pl


if __name__ == "__main__":
    app.run()
