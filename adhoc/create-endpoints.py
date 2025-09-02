import marimo

__generated_with = "0.15.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


@app.cell
def _(pl):
    targets = pl.read_parquet("adhoc/acs_variable_targets.parquet")
    targets.head()
    return (targets,)


@app.cell
def _(pl, targets):
    cleanup = targets.with_columns(
        pl.col("almost_group").str.split(by="_").list.first().alias("group")
    ).drop("almost_group")

    cleanup.head()
    return (cleanup,)


@app.cell
def _(pl):
    years = pl.DataFrame({"year": [2021, 2022, 2023]})
    return (years,)


@app.cell
def _(cleanup, pl, years):
    pre_group_string = "?get=group("
    post_group_string = ")&for=state:09"

    crossed = (
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
                    pl.col("group"),
                    pl.lit(post_group_string),
                ]
            ).alias("post_base_string"),
        )
        .with_columns(
            pl.concat_str([pl.col("base"), pl.col("post_base_string")]).alias(
                "alt_group_base"
            )
        )
    )
    crossed.head()
    return (crossed,)


@app.cell
def _(mo):
    mo.md("""# For Group-based Endpoints:""")
    return


@app.cell
def _(crossed, pl):
    group_based_urls = crossed.select(pl.col("alt_group_base").unique())
    group_based_urls.head()
    return (group_based_urls,)


@app.cell
def _(group_based_urls):
    group_based_urls.write_parquet("adhoc/group-based-urls.parquet")
    return


@app.cell
def _(mo):
    mo.md("""# For Variable Sets:""")
    return


@app.cell
def _(crossed):
    crossed.select("base").unique()
    return


@app.cell
def _(crossed):
    crossed.select("variable_id").unique()
    return


@app.cell
def _(crossed):
    minimum = crossed.select(["variable_id", "base"])
    minimum.head()
    return (minimum,)


@app.cell
def _(minimum):
    grouped = minimum.group_by("base")
    return (grouped,)


@app.cell
def _(grouped):
    all_urls = []
    total_vars = 49

    for _base, _data in grouped:
        variable_ids = _data.select("variable_id").unique().to_series().to_list()
        (url, *_) = _base

        for i in range(0, len(variable_ids), total_vars):
            chunk = variable_ids[i : i + total_vars]

            url_chunks = f"{url}?get=NAME,{','.join(chunk)}&for=state:09"
            all_urls.append(url_chunks)

    all_urls
    return (all_urls,)


@app.cell
def _(all_urls, pl):
    pl.DataFrame({"urls": all_urls}).write_parquet("adhoc/chunked_urls.parquet")
    return


if __name__ == "__main__":
    app.run()
