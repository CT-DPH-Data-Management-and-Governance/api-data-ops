from pydantic_settings import BaseSettings, SettingsConfigDict

import dataops.models.configs as cfg


class AppSettings(BaseSettings):
    """
    Defines application settings for interacting with the portal platform and Census API.
    """

    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__")

    account: cfg.AccountConfig
    api: cfg.APIConfig
    census: cfg.CensusConfig
