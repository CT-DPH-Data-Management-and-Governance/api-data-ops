# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "polars==1.33.0",
# ]
# ///

import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md("""# Adhoc Endpoint Puller""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""## Read-in Parquet""")
    return


@app.cell
def _(mo):
    parquet_selector = mo.ui.file(
        filetypes=[".parquet"], label="Upload Parquet file"
    )

    parquet_selector
    return (parquet_selector,)


@app.cell
def _(mo, parquet_selector, pl):
    parquet_contents = parquet_selector.contents()


    def if_not_stop() -> None:
        if not parquet_contents:
            mo.stop(True)


    if_not_stop()
    data = pl.scan_parquet(parquet_contents)
    return data, if_not_stop


@app.cell
def _(if_not_stop, mo):
    if_not_stop()
    mo.md("""## Endpoint Preview""")
    return


@app.cell
def _(data, if_not_stop):
    if_not_stop()
    data.head().collect()
    return


@app.cell
def _(cs, data, if_not_stop, pl):
    # convert to list
    if_not_stop()
    endpoints = (
        data.select(cs.contains("endpoint", "url"))
        .select(pl.first())
        .collect()
        .to_series()
        .to_list()
    )
    return (endpoints,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""## Data Pull""")
    return


@app.cell
def _(APIData, APIEndpoint, endpoints, if_not_stop, pl):
    if_not_stop()

    all_frames = []

    for endpoint in endpoints:
        endpoint = APIEndpoint.from_url(endpoint)
        endpoint_data = APIData(endpoint=endpoint).long()
        all_frames.append(endpoint_data)

    output = pl.concat(all_frames)
    return (output,)


@app.cell
def _(if_not_stop, mo):
    if_not_stop()
    mo.md("Output Preview")
    return


@app.cell
def _(if_not_stop, output):
    if_not_stop()
    output.head(25).collect()
    return


@app.cell
def _(if_not_stop, mo):
    if_not_stop()

    parquet_export_button = mo.ui.run_button(
        tooltip="Export Data to parquet file.", label="Export Data"
    )

    parquet_export_button
    return (parquet_export_button,)


@app.cell
def _(output, parquet_export_button):
    if parquet_export_button.value:
        output.collect().write_parquet("adhoc-data-pull.parquet")
    return


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import polars as pl
    from polars import selectors as cs

    from dataops.apis.acs import APIEndpoint, APIData
    return APIData, APIEndpoint, cs, mo, pl


if __name__ == "__main__":
    app.run()
