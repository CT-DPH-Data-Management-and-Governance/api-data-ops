import polars as pl
from sodapy import Socrata

from dataops.settings.socrata import AppSettings


def fetch_data(
    source: str | None = None,
    settings: AppSettings | None = None,
    lazy: bool = True,
) -> pl.LazyFrame | pl.DataFrame:
    """
    Retrieve portal data as polars dataframe.
    Environmental variables are used as defaults unless otherwise specified.
    """
    if settings is None:
        settings = AppSettings()

    if source is None:
        source = settings.source_id

    with Socrata(
        settings.domain,
        settings.socrata_token,
        settings.socrata_user,
        settings.socrata_pass,
    ) as client:
        data = client.get_all(source)
        data = pl.LazyFrame(data)

    if not lazy:
        return data.collect()

    return data


def pull_endpoints(df: pl.DataFrame) -> list[str] | pl.DataFrame:
    """Retrieve a list of api endpoints from a dataframe."""

    if "endpoint" in df.columns:
        return df.select(pl.col("endpoint").struct.unnest()).to_series().to_list()

    return df


def replace_data(
    data: pl.DataFrame,
    target: str | None = None,
    settings: AppSettings | None = None,
):
    if settings is None:
        settings = AppSettings()

    if target is None:
        target = settings.target_id

    dict_data = data.to_dicts()

    with Socrata(
        settings.domain,
        settings.socrata_token,
        settings.socrata_user,
        settings.socrata_pass,
    ) as client:
        client.replace(target, dict_data)
