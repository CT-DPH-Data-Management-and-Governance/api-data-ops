import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md("""# Adhoc ACS Endpoint Creator""")
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Parameters
    ------------
    """
    )
    return


@app.cell
def _(mo):
    year_select = mo.ui.multiselect(
        options=[2021, 2022, 2023], label="Select Years:"
    )
    year_select
    return (year_select,)


@app.cell
def _(mo):
    parquet_selector = mo.ui.file(
        filetypes=[".parquet"], label="Select Parquet file"
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
    data_cols = data.collect_schema().names()
    return data, data_cols, if_not_stop


@app.cell
def _(if_not_stop, mo):
    if_not_stop()
    mo.md("### Parquet Preview")
    return


@app.cell
def _(data):
    data.head(15).collect()
    return


@app.cell
def _(if_not_stop, mo):
    if_not_stop()
    mo.md("### Specify Target Column Names")
    return


@app.cell
def _(data_cols, if_not_stop, mo):
    if_not_stop()
    var_id_dropdown = mo.ui.dropdown(
        options=data_cols,
        label="Select the ACS Variable ID Column Name:",
    )
    var_id_dropdown
    return (var_id_dropdown,)


@app.cell
def _(data_cols, if_not_stop, mo):
    if_not_stop()
    dataset_dropdown = mo.ui.dropdown(
        options=data_cols,
        label="Select the ACS Dataset Column Name:",
    )
    dataset_dropdown
    return (dataset_dropdown,)


@app.cell
def _(data, dataset_dropdown, pl, var_id_dropdown):
    cleanup = (
        data.select(
            pl.col(var_id_dropdown.value).alias("variable_id"),
            pl.col(dataset_dropdown.value).alias("dataset"),
        )
        .unique()
        .with_columns(
            pl.col("variable_id")
            .str.split(by="_")
            .list.first()
            .alias("computed_group"),
        )
    )
    return (cleanup,)


@app.cell
def _(if_not_stop, mo):
    if_not_stop()
    mo.md("### Paremeter Preview")
    return


@app.cell
def _(cleanup):
    cleanup.head().collect()
    return


@app.cell
def _(cleanup, if_not_stop, pl, year_select):
    if_not_stop()

    years = pl.LazyFrame({"year": year_select.value})

    pre_group_string = "?get=group("
    post_group_string = ")&for=state:09"

    final = (
        cleanup.join(years, how="cross")
        .with_columns(
            pl.concat_str(
                [
                    pl.lit("https://api.census.gov/data/"),
                    pl.col("year"),
                    pl.lit("/"),
                    pl.col("dataset"),
                ]
            )
            .str.strip_chars("/")
            .alias("base"),
            pl.concat_str(
                [
                    pl.lit(pre_group_string),
                    pl.col("computed_group"),
                    pl.lit(post_group_string),
                ]
            ).alias("post_base_string"),
        )
        .with_columns(
            pl.concat_str([pl.col("base"), pl.col("post_base_string")]).alias(
                "group_based_endpoint"
            )
        )
    )
    return (final,)


@app.cell
def _(if_not_stop, mo):
    if_not_stop()
    mo.md("""### Group-based Endpoints:""")
    return


@app.cell
def _(final, pl):
    group_based_urls = final.select(pl.col("group_based_endpoint").unique().sort())
    return (group_based_urls,)


@app.cell
def _(group_based_urls):
    group_based_urls.head().collect()
    return


@app.cell
def _(if_not_stop, mo):
    if_not_stop()

    export_group_urls = mo.ui.run_button(label="Export group based urls")
    export_group_urls
    return (export_group_urls,)


@app.cell
def _(export_group_urls, group_based_urls, if_not_stop):
    if_not_stop()

    if export_group_urls.value:
        group_based_urls.sink_parquet("group-based-urls.parquet")
    return


@app.cell
def _(if_not_stop, mo):
    if_not_stop()
    mo.md("""### Variable Sets:""")
    return


@app.cell
def _(final, if_not_stop, pl):
    if_not_stop()

    grouped = final.select(["variable_id", "base"]).collect().group_by("base")

    all_urls = []
    total_vars = 49

    for _base, _data in grouped:
        variable_ids = _data.select("variable_id").unique().to_series().to_list()
        (url, *_) = _base

        for i in range(0, len(variable_ids), total_vars):
            chunk = variable_ids[i : i + total_vars]

            url_chunks = f"{url}?get=NAME,{','.join(chunk)}&for=state:09"
            all_urls.append(url_chunks)

    variable_set = pl.LazyFrame({"variable_set_endpoints": all_urls})
    variable_set.head().collect()
    return (variable_set,)


@app.cell
def _(if_not_stop, mo):
    if_not_stop()

    export_variable_set_urls = mo.ui.run_button(
        label="Export variable set based urls"
    )
    export_variable_set_urls
    return (export_variable_set_urls,)


@app.cell
def _(export_variable_set_urls, variable_set):
    if export_variable_set_urls.value:
        variable_set.sink_parquet("variable-based-urls.parquet")
    return


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


if __name__ == "__main__":
    app.run()
