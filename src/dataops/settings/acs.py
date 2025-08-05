from pydantic_settings import BaseSettings, SettingsConfigDict

from dataops.configs.account import AccountConfig
from dataops.configs.census import CensusConfig
from dataops.configs.socrata import SocrataAPIConfig


class AppSettings(BaseSettings):
    """
    Defines application settings for interacting with the portal platform and Census API.
    """

    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__")

    account: AccountConfig
    api: SocrataAPIConfig
    census: CensusConfig
