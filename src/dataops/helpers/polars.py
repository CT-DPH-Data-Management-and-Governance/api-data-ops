import polars as pl


def ensure_column_exists(
    data: pl.LazyFrame | pl.DataFrame,
    column_name: list[str],
    default_value: str = "unknown as queried",
) -> pl.LazyFrame:
    for col_name in column_name:
        if col_name not in data.collect_schema().names():
            data = data.with_columns(pl.lit(default_value).alias(col_name))

    return data
