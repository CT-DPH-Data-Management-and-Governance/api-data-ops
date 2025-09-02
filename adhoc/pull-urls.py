import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md("""# Adhoc""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""## Data Read""")
    return


@app.cell
def _(pl):
    acs_variable_targets = pl.read_parquet("adhoc/acs_variable_targets.parquet")
    endpoints = (
        pl.read_parquet("adhoc/group-based-urls.parquet").to_series().to_list()
    )
    return acs_variable_targets, endpoints


@app.cell
def _(endpoints):
    endpoints[0:5]
    return


@app.cell
def _(acs_variable_targets):
    acs_variable_targets.head()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""## Data Pull""")
    return


@app.cell
def _(APIData, APIEndpoint, endpoints, pl):
    all_frames = []

    for endpoint in endpoints:
        endpoint = APIEndpoint.from_url(endpoint)
        endpoint_data = APIData(endpoint=endpoint).long()
        all_frames.append(endpoint_data)

    all_frames = pl.concat(all_frames)
    return (all_frames,)


@app.cell
def _(all_frames):
    all_frames.head().collect()
    return


@app.cell
def _(acs_variable_targets, all_frames):
    # ignoring that we lose all the stratifiers for a moment...
    no_strats = all_frames.join(
        acs_variable_targets.lazy(),
        how="inner",
        left_on="variable",
        right_on="variable_id",
    )
    no_strats.head().collect()
    return (no_strats,)


@app.cell
def _(no_strats):
    no_strats.sink_parquet("adhoc/no_strats.parquet")
    return


@app.cell
def _(all_frames):
    all_frames.sink_parquet("adhoc/adhoc.parquet")
    return


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import polars as pl

    from dataops.apis.acs import APIEndpoint, APIData
    return APIData, APIEndpoint, mo, pl


if __name__ == "__main__":
    app.run()
